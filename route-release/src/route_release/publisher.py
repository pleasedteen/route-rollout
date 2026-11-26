"""发布工具: 值班人员的统一入口。

设计要点:
  * 进程不持有权威状态。权威状态是(1)状态文件 (2)各服务 /status 的实际版本。
  * 每个 tick 先 reconcile(按服务实际版本纠正本地进度), 再决定是否下发。
    确认丢失后重启新进程, reconcile 会发现服务其实已经切换, 直接续跑。
  * 上一批全部生效才开启下一批; 暂停只停"开启新批次", 在途操作等出结果。
  * apply 按规则内容指纹幂等, 重发安全。
"""
from __future__ import annotations

import argparse
import json
import signal
import time
import urllib.error
import urllib.request
from datetime import datetime
from typing import Dict, List, Mapping, Optional, Tuple

from .model import (
    Ruleset,
    Topology,
    ruleset_to_plain,
    version_token,
)
from .planner import Plan, load_ruleset_file, make_plan
from .state import ReleaseStore

POLL_INTERVAL = 0.3
MAX_CONSECUTIVE_ERRORS = 5
APPLY_MAX_ATTEMPTS = 4


# ---------------------------------------------------------------- HTTP 小工具

def http_json(url: str, payload: Optional[Mapping] = None,
              timeout: float = 5.0) -> Tuple[int, dict]:
    data = None
    headers = {"Accept": "application/json"}
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(url, data=data, headers=headers,
                                 method="POST" if payload is not None else "GET")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status, json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read().decode("utf-8"))
    except Exception as exc:
        raise RuntimeError(f"HTTP 调用 {url} 失败: {type(exc).__name__}: {exc}")


def load_registry(path: str) -> Dict[str, str]:
    with open(path, "r", encoding="utf-8") as fh:
        return json.load(fh)


def now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


# ------------------------------------------------------------------- 发布器

class Publisher:
    def __init__(self, store: ReleaseStore):
        self.store = store
        data = store.read()
        self.data = data
        self.topo = Topology.from_dict(self._load_json(data["topology_path"]))
        self.old_rules = load_ruleset_file(data["old_rules_path"])
        self.new_rules = load_ruleset_file(data["new_rules_path"])
        self.registry = load_registry(data["registry_path"])
        self.forward_plan = Plan.from_dict(data["forward_plan"])
        self.rollback_plan = (Plan.from_dict(data["rollback_plan"])
                              if data.get("rollback_plan") else None)
        self.stop_requested = False

    @staticmethod
    def _load_json(path: str) -> dict:
        with open(path, "r", encoding="utf-8") as fh:
            return json.load(fh)

    # ---- 对单台服务的操作 -------------------------------------------------

    def service_status(self, node: str) -> dict:
        _, body = http_json(self.registry[node] + "/status")
        return body

    def apply_node(self, node: str, rules: Ruleset) -> dict:
        code, body = http_json(self.registry[node] + "/apply",
                               {"rules": ruleset_to_plain(rules)})
        if code != 200 or "version" not in body:
            raise RuntimeError(f"{node} apply 返回 {code}: {body}")
        return body

    def probe_node(self, node: str, cls: str) -> dict:
        return http_json(self.registry[node] + "/probe",
                         {"class": cls, "path": [], "visited": []})[1]

    # ---- 实际版本对账 -----------------------------------------------------

    def reconcile(self) -> Tuple[Dict[str, str], List[str]]:
        """查询所有服务的实际生效版本, 返回 (节点->实际token, 不可用节点)。"""
        actual: Dict[str, str] = {}
        unreachable: List[str] = []
        for node in self.topo.nodes:
            try:
                actual[node] = self.service_status(node)["version"]
            except RuntimeError as exc:
                unreachable.append(f"{node}({exc})")
        # 以服务实际版本为准更新 applied
        applied = dict(self.data.get("applied", {}))
        for node, tok in actual.items():
            applied[node] = tok
        self.data = self.store.update(applied=applied)
        return actual, unreachable

    def token_map(self, rules: Mapping[str, Ruleset]) -> Dict[str, str]:
        return {n: version_token(rules[n]) for n in self.topo.nodes}

    # ---- 单步推进 ---------------------------------------------------------

    def tick(self) -> str:
        """推进一次。返回当前 phase。"""
        self.data = self.store.read()
        phase = self.data["phase"]
        actual, unreachable = self.reconcile()
        if unreachable:
            self.data = self.store.update(
                last_error=f"{now_str()} 以下服务不可达: {'; '.join(unreachable)}")
            return phase  # 不动配置, 下个 tick 重试; 连续不可达由 run 循环计数

        old_tok = self.token_map(self.old_rules)
        new_tok = self.token_map(self.new_rules)

        if phase == "FORWARD_COMPLETE":
            return phase
        if phase == "ROLLED_BACK":
            return phase
        # PAUSED/FAILED/RUNNING 按当前方向分派; 回退中途暂停也走回退循环
        if self.data["direction"] == "rollback":
            return self._tick_rollback(actual, old_tok, new_tok, phase)
        return self._tick_forward(actual, old_tok, new_tok, phase)

    def _target_token(self, node: str, old_tok, new_tok) -> str:
        direction = self.data["direction"]
        return new_tok[node] if direction == "forward" else old_tok[node]

    def _active_plan(self) -> Plan:
        return (self.rollback_plan if self.data["direction"] == "rollback"
                else self.forward_plan)

    def _tick_forward(self, actual, old_tok, new_tok, phase) -> str:
        plan = self.forward_plan.batches
        idx = self.data["current_batch_index"]
        applied = self.data["applied"]

        if not plan:
            if all(applied.get(n) == new_tok[n] for n in self.topo.nodes):
                self.data = self.store.update(phase="FORWARD_COMPLETE",
                                              last_error=None)
                return "FORWARD_COMPLETE"

        if idx >= len(plan):
            if all(applied.get(n) == new_tok[n] for n in self.topo.nodes):
                self.data = self.store.update(phase="FORWARD_COMPLETE",
                                              last_error=None)
                return "FORWARD_COMPLETE"
            # 所有批次都发过但版本不齐: 继续对账补齐(典型的确认丢失场景)
            self._drive_batch(set(self.topo.nodes), old_tok, new_tok)
            return phase

        current = set(plan[idx])
        # 先确认之前的批次都已整体生效, 再处理当前批
        prev_done = all(
            applied.get(n) == new_tok[n]
            for i in range(idx) for n in plan[i]
        )
        if not prev_done:
            # 进程重启后的缝隙: 把欠账批次补齐, 仍不开新批
            for i in range(idx):
                self._drive_batch(set(plan[i]), old_tok, new_tok)
            return phase

        self._drive_batch(current, old_tok, new_tok)
        applied = self.store.read()["applied"]
        if all(applied.get(n) == new_tok[n] for n in current):
            if phase == "PAUSED":
                # 暂停语义: 当前批可以收尾, 但绝不开启下一批, 进度留待 resume
                return "PAUSED"
            self.data = self.store.update(current_batch_index=idx + 1)
            if idx + 1 == len(plan):
                if all(applied.get(n) == new_tok[n] for n in self.topo.nodes):
                    self.data = self.store.update(phase="FORWARD_COMPLETE",
                                                  last_error=None)
                    return "FORWARD_COMPLETE"
            return self.store.read()["phase"]
        # 当前批未齐: 暂停态不再开启新批次(本来也没开), 等下个 tick
        return phase

    def _tick_rollback(self, actual, old_tok, new_tok, phase="ROLLING_BACK") -> str:
        plan = self.rollback_plan.batches  # type: ignore[union-attr]
        idx = self.data["current_batch_index"]
        applied = self.data["applied"]

        if idx >= len(plan) or not plan:
            if all(applied.get(n) == old_tok[n] for n in self.topo.nodes):
                self.data = self.store.update(phase="ROLLED_BACK", last_error=None)
                return "ROLLED_BACK"
            self._drive_batch(set(self.topo.nodes), old_tok, new_tok)
            return "ROLLING_BACK"

        current = set(plan[idx])
        prev_done = all(
            applied.get(n) == old_tok[n]
            for i in range(idx) for n in plan[i]
        )
        if not prev_done:
            for i in range(idx):
                self._drive_batch(set(plan[i]), old_tok, new_tok)
            return "ROLLING_BACK"

        self._drive_batch(current, old_tok, new_tok)
        applied = self.store.read()["applied"]
        if all(applied.get(n) == old_tok[n] for n in current):
            if phase == "PAUSED":
                return "PAUSED"
            self.data = self.store.update(current_batch_index=idx + 1)
            if idx + 1 == len(plan):
                if all(applied.get(n) == old_tok[n] for n in self.topo.nodes):
                    self.data = self.store.update(phase="ROLLED_BACK",
                                                  last_error=None)
                    return "ROLLED_BACK"
        return "ROLLING_BACK"

    def _drive_batch(self, nodes: set, old_tok, new_tok) -> None:
        """把一批服务推进到各自目标版本; 已在目标版本的跳过。

        同批顺序无所谓(规划已枚举验证任意子集安全)。暂停时调用方只会传入
        已开启的批次, 因此本函数不会在暂停时开启新批次。
        """
        direction = self.data["direction"]
        rules = self.new_rules if direction == "forward" else self.old_rules
        target_tok = new_tok if direction == "forward" else old_tok
        for node in sorted(nodes):
            current = self.data["applied"].get(node)
            if current == target_tok[node]:
                continue
            # 再查一次实时状态, 处理"确认丢失但实际已生效"
            live = self.service_status(node)
            if live["version"] == target_tok[node]:
                self.data = self.store.update(
                    applied={**self.store.read()["applied"],
                             node: target_tok[node]})
                print(f"  [{now_str()}] {node}: 实际已是目标版本"
                      f"(可能上次确认丢失), 对账确认", flush=True)
                continue
            # apply 可能遇到丢请求/丢确认: 有限重试, 每次重试前先查真实状态,
            # 借助服务端幂等(按规则指纹去重)保证重发安全。
            resp = None
            for attempt in range(APPLY_MAX_ATTEMPTS):
                try:
                    resp = self.apply_node(node, rules[node])
                    break
                except RuntimeError as exc:
                    print(f"  [{now_str()}] {node}: apply 第 {attempt + 1} 次"
                          f"失败: {exc}", flush=True)
                    time.sleep(POLL_INTERVAL)
                    # 失败也可能其实已生效(确认丢失): 查真实状态
                    try:
                        live = self.service_status(node)
                    except RuntimeError:
                        live = None
                    if live and live["version"] == target_tok[node]:
                        resp = live
                        break
            if resp is None:
                # 本节点暂时推不动: 抛出由 run 循环计数, 下个 tick 再试, 不开新批
                raise RuntimeError(f"{node} 连续 {APPLY_MAX_ATTEMPTS} 次 apply 失败")
            if resp.get("duplicate"):
                mark = "重复请求, 服务幂等忽略"
            elif "changed" in resp:
                mark = "已切换"
            else:
                mark = "对账确认生效"
            print(f"  [{now_str()}] {node}: apply -> {resp['version']} ({mark})",
                  flush=True)
            self.data = self.store.update(
                applied={**self.store.read()["applied"],
                         node: resp["version"]})

    # ---- 运行循环 ---------------------------------------------------------

    def run(self, wait: bool = True, max_seconds: float = 300.0) -> str:
        # PAUSED 由外部 pause 命令写入: 本进程观察到后收尾退出, 进度保留在状态文件
        stop_phases = {"FORWARD_COMPLETE", "ROLLED_BACK", "PAUSED"}
        deadline = time.monotonic() + max_seconds
        consecutive_errors = 0
        while not self.stop_requested:
            try:
                phase = self.tick()
                consecutive_errors = 0
            except RuntimeError as exc:
                consecutive_errors += 1
                print(f"  [{now_str()}] 调用失败({consecutive_errors}/"
                      f"{MAX_CONSECUTIVE_ERRORS}): {exc}", flush=True)
                if consecutive_errors >= MAX_CONSECUTIVE_ERRORS:
                    self.store.update(phase="FAILED", last_error=str(exc))
                    print("  连续失败过多, 标记 FAILED; 服务恢复后可用 resume 继续",
                          flush=True)
                    return "FAILED"
                time.sleep(POLL_INTERVAL)
                continue
            if phase in stop_phases:
                return phase
            if not wait:
                return phase
            if time.monotonic() > deadline:
                self.store.update(last_error="超过 max_seconds, 停止等待")
                return self.store.read()["phase"]
            time.sleep(POLL_INTERVAL)
        return self.store.read()["phase"]

    # ---- 回退计划 ---------------------------------------------------------

    def ensure_rollback_plan(self, max_batch_size: int,
                             max_batches: int) -> Plan:
        if self.rollback_plan is not None:
            return self.rollback_plan
        # 以服务的实际生效配置作为回退起点(可能是新老混合),
        # 规划器枚举的混合状态才能覆盖真实的部分生效现场。
        actual, unreachable = self.reconcile()
        if unreachable:
            raise SystemExit(f"存在不可达服务, 无法安全规划回退: "
                             f"{'; '.join(unreachable)}")
        old_tok = self.token_map(self.old_rules)
        new_tok = self.token_map(self.new_rules)
        current_rules: Dict[str, Ruleset] = {}
        for node in self.topo.nodes:
            tok = actual[node]
            if tok == old_tok[node]:
                current_rules[node] = self.old_rules[node]
            elif tok == new_tok[node]:
                current_rules[node] = self.new_rules[node]
            else:
                raise SystemExit(
                    f"节点 {node} 的实际版本 {tok} 既非发布前也非目标版本, "
                    f"拒绝自动回退, 请人工核查")
        plan = make_plan(self.topo, current_rules, self.old_rules,
                         max_batch_size, max_batches, direction="rollback")
        if not plan.ok:
            raise SystemExit(f"回退无解, 原配置保持不变: {plan.reason}")
        self.data = self.store.update(rollback_plan=plan.to_dict())
        self.rollback_plan = plan
        return plan


# --------------------------------------------------------------------- 命令

def cmd_submit(args) -> None:
    topo = Topology.from_dict(json.load(open(args.topology, encoding="utf-8")))
    old = load_ruleset_file(args.old_rules)
    new = load_ruleset_file(args.new_rules)
    plan = make_plan(topo, old, new, args.max_batch_size, args.max_batches)
    print("=" * 68)
    print("发布计划(动配置前生成, 此时不触碰任何服务)")
    print("=" * 68)
    if not plan.ok:
        print(f"无解: {plan.reason}")
        print("原配置保留, 未创建任何发布状态。")
        raise SystemExit(2)
    for i, batch in enumerate(plan.batches, 1):
        print(f"  批次 {i:>2}: {', '.join(batch)}")
    print(f"  批数 {len(plan.batches)} <= 上限 {args.max_batches}; "
          f"单批规模 {max(len(b) for b in plan.batches)} <= 上限 "
          f"{args.max_batch_size}")
    print("  安全性: 已枚举每批的任意部分生效子集, 混合状态下各类流量均可达固定出口")
    store = ReleaseStore(args.state)
    store.create(release_id=args.release_id, topology_path=args.topology,
                 old_rules_path=args.old_rules, new_rules_path=args.new_rules,
                 plan=plan, registry_path=args.registry)
    print(f"状态已写入 {args.state}, 可用 run 开始发布。")


def cmd_run(args) -> None:
    store = ReleaseStore(args.state)
    pub = Publisher(store)
    _install_signals(pub)
    phase = pub.run(max_seconds=args.max_seconds)
    print(f"[{now_str()}] run 结束, 当前阶段: {phase}")


def cmd_resume(args) -> None:
    store = ReleaseStore(args.state)
    data = store.read()
    if data["phase"] == "FAILED":
        store.update(phase="RUNNING", last_error=None)
    elif data["phase"] == "PAUSED":
        store.update(phase="RUNNING")
    pub = Publisher(store)
    _install_signals(pub)
    phase = pub.run(max_seconds=args.max_seconds)
    print(f"[{now_str()}] resume 结束, 当前阶段: {phase}")


def cmd_pause(args) -> None:
    store = ReleaseStore(args.state)
    data = store.read()
    if data["phase"] not in ("RUNNING", "FAILED", "ROLLING_BACK"):
        print(f"当前阶段 {data['phase']} 无需暂停")
        return
    store.update(phase="PAUSED")
    print(f"[{now_str()}] 已请求暂停: 不再开启新批次, 等待在途操作出结果...")
    pub = Publisher(store)
    old_tok = pub.token_map(pub.old_rules)
    new_tok = pub.token_map(pub.new_rules)
    known = set(old_tok.values()) | set(new_tok.values())
    deadline = time.monotonic() + args.timeout
    while time.monotonic() < deadline:
        actual, unreachable = pub.reconcile()
        if not unreachable and all(tok in known for tok in actual.values()):
            print(f"[{now_str()}] 在途操作均已明确, 进度已保存, 可 resume 或 rollback")
            return
        time.sleep(POLL_INTERVAL)
    print("暂停等待超时, 状态已保存; 服务恢复后再次 pause 或直接 resume")


def cmd_rollback(args) -> None:
    store = ReleaseStore(args.state)
    data = store.read()
    if data["phase"] == "ROLLED_BACK":
        print("已经处于回退完成状态")
        return
    if data["phase"] == "ROLLING_BACK":
        print("回退已在进行, 直接续跑")
    pub = Publisher(store)
    plan = pub.ensure_rollback_plan(args.max_batch_size, args.max_batches)
    print("回退计划(同样受单批规模与可达性约束):")
    for i, batch in enumerate(plan.batches, 1):
        print(f"  回退批 {i:>2}: {', '.join(batch)}")
    store.update(phase="ROLLING_BACK", direction="rollback",
                 current_batch_index=0)
    _install_signals(pub)
    phase = pub.run(max_seconds=args.max_seconds)
    print(f"[{now_str()}] rollback 结束, 当前阶段: {phase}")


def cmd_status(args) -> None:
    store = ReleaseStore(args.state)
    data = store.read()
    pub = Publisher(store)
    old_tok = pub.token_map(pub.old_rules)
    new_tok = pub.token_map(pub.new_rules)
    print("=" * 68)
    print(f"发布 {data['release_id']}  阶段: {data['phase']}  "
          f"方向: {data['direction']}")
    print("=" * 68)
    plan = (pub.rollback_plan if data["direction"] == "rollback"
            and pub.rollback_plan else pub.forward_plan)
    for i, batch in enumerate(plan.batches, 1):
        print(f"  批次 {i:>2}: {', '.join(batch)}")
    print("-" * 68)
    print(f"{'服务':<10}{'实际版本':<22}{'发布前':<10}{'目标':<10}状态")
    actual, unreachable = pub.reconcile()
    for node in pub.topo.nodes:
        tok = actual.get(node, "<不可达>")
        if tok == new_tok[node]:
            mark = "新配置已生效"
        elif tok == old_tok[node]:
            mark = "发布前配置"
        else:
            mark = "未知版本"
        short = lambda t: t[:18]  # noqa: E731
        print(f"{node:<10}{short(tok):<22}{short(old_tok[node]):<10}"
              f"{short(new_tok[node]):<10}{mark}")
    if unreachable:
        print("不可达: " + "; ".join(unreachable))
    if data.get("last_error"):
        print(f"最近错误: {data['last_error']}")
    idx = data["current_batch_index"]
    print(f"批次进度: {min(idx, len(plan.batches))}/{len(plan.batches)}")


def cmd_verify(args) -> None:
    """从每个服务、每类流量发起真实逐跳探测, 检查是否到达固定出口。"""
    store = ReleaseStore(args.state)
    pub = Publisher(store)
    failures = 0
    print("=" * 68)
    print("实时可达性验证(服务间逐跳真实转发)")
    print("=" * 68)
    for cls in pub.topo.classes:
        egress = pub.topo.egress_of[cls]
        for start in pub.topo.nodes:
            res = pub.probe_node(start, cls)
            if res.get("ok") and res.get("egress") == egress:
                print(f"  {start} [{cls}] -> {' -> '.join(res['path'])} "
                      f"=> 固定出口 {egress}  OK")
            else:
                failures += 1
                print(f"  {start} [{cls}] 失败: {res.get('error')}")
    print("-" * 68)
    print("全部可达" if failures == 0 else f"{failures} 条探测失败")
    raise SystemExit(1 if failures else 0)


def cmd_plan_only(args) -> None:
    """只做规划不落状态, 用于无解场景演示。"""
    topo = Topology.from_dict(json.load(open(args.topology, encoding="utf-8")))
    old = load_ruleset_file(args.old_rules)
    new = load_ruleset_file(args.new_rules)
    plan = make_plan(topo, old, new, args.max_batch_size, args.max_batches)
    if plan.ok:
        for i, batch in enumerate(plan.batches, 1):
            print(f"批次 {i}: {list(batch)}")
    else:
        print(f"无解: {plan.reason}")
        raise SystemExit(2)


def _install_signals(pub: Publisher) -> None:
    def handler(signum, frame):
        print(f"\n[{now_str()}] 收到信号 {signum}, 请求停止(进度已持续落盘)...")
        pub.stop_requested = True
    signal.signal(signal.SIGINT, handler)
    signal.signal(signal.SIGTERM, handler)


def build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(description="本地转发服务批量发布工具")
    sub = ap.add_subparsers(dest="cmd", required=True)

    p = sub.add_parser("submit", help="校验并生成完整计划, 创建发布状态")
    p.add_argument("--topology", required=True)
    p.add_argument("--old-rules", required=True)
    p.add_argument("--new-rules", required=True)
    p.add_argument("--registry", required=True)
    p.add_argument("--state", required=True)
    p.add_argument("--release-id", default="rel-1")
    p.add_argument("--max-batch-size", type=int, required=True)
    p.add_argument("--max-batches", type=int, required=True)
    p.set_defaults(func=cmd_submit)

    p = sub.add_parser("run", help="按计划发布(可反复执行, 自动对账续跑)")
    p.add_argument("--state", required=True)
    p.add_argument("--max-seconds", type=float, default=300)
    p.set_defaults(func=cmd_run)

    p = sub.add_parser("resume", help="暂停/失败后继续发布")
    p.add_argument("--state", required=True)
    p.add_argument("--max-seconds", type=float, default=300)
    p.set_defaults(func=cmd_resume)

    p = sub.add_parser("pause", help="暂停: 不开新批, 等在途操作明确并保存进度")
    p.add_argument("--state", required=True)
    p.add_argument("--timeout", type=float, default=30)
    p.set_defaults(func=cmd_pause)

    p = sub.add_parser("rollback", help="回退到发布前配置(重新做受约束规划)")
    p.add_argument("--state", required=True)
    p.add_argument("--max-batch-size", type=int, default=0)
    p.add_argument("--max-batches", type=int, default=0)
    p.add_argument("--max-seconds", type=float, default=300)
    p.set_defaults(func=cmd_rollback)

    p = sub.add_parser("status", help="查询发布进度与各服务实际生效版本")
    p.add_argument("--state", required=True)
    p.set_defaults(func=cmd_status)

    p = sub.add_parser("verify", help="逐跳探测验证所有流量可达固定出口")
    p.add_argument("--state", required=True)
    p.set_defaults(func=cmd_verify)

    p = sub.add_parser("plan", help="只规划不落地(演示无解原因)")
    p.add_argument("--topology", required=True)
    p.add_argument("--old-rules", required=True)
    p.add_argument("--new-rules", required=True)
    p.add_argument("--max-batch-size", type=int, required=True)
    p.add_argument("--max-batches", type=int, required=True)
    p.set_defaults(func=cmd_plan_only)
    return ap


def main(argv=None) -> None:
    args = build_parser().parse_args(argv)
    if args.cmd == "rollback" and args.max_batch_size == 0:
        data = ReleaseStore(args.state).read()
        args.max_batch_size = data["forward_plan"]["max_batch_size"]
        args.max_batches = data["forward_plan"]["max_batches"]
    args.func(args)


if __name__ == "__main__":
    main()

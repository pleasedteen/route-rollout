"""发布工具: 值班人员的统一入口。

设计要点:
  * 进程不持有权威状态。权威状态是(1)状态文件 (2)各服务 /status 的实际版本。
  * 每个 tick 先 reconcile(按服务实际版本纠正本地进度, 并结算已可见的未决
    操作), 再决定是否下发。
  * 下发任何 apply 之前, 先在状态文件里登记 pending(已发出未确认)并分配一个
    单调递增的 epoch; 服务的栅栏代数保证旧 epoch 的迟到/重发请求不会覆盖新结果。
  * 暂停一经接受(原子写 PAUSED + 抬高 epoch), 任何进程都无法再登记新操作,
    因此不会开启新批次; 在途操作要等到"可见生效"或"被更高栅栏永久拒止"才算
    暂停完成。超时则保留未决状态, status 可查。
  * 上一批全部生效(且无未决)才开启下一批。
  * 回退只在正向发布或暂停完成后, 以各节点实际生效规则为起点重新规划,
    同样枚举部分生效子集、受单批规模与总批数约束。
"""
from __future__ import annotations

import argparse
import json
import signal
import time
import urllib.error
import urllib.request
from datetime import datetime
from typing import Dict, List, Mapping, Optional, Sequence, Tuple

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


def short(tok: Optional[str], n: int = 10) -> str:
    return (tok or "<none>")[:n]


# ------------------------------------------------------------------- 发布器

class Publisher:
    def __init__(self, store: ReleaseStore):
        self.store = store
        self.data = store.read()
        self.topo = Topology.from_dict(self._load_json(self.data["topology_path"]))
        self.old_rules = load_ruleset_file(self.data["old_rules_path"])
        self.new_rules = load_ruleset_file(self.data["new_rules_path"])
        self.registry = load_registry(self.data["registry_path"])
        self.forward_plan = Plan.from_dict(self.data["forward_plan"])
        self.rollback_plan = (Plan.from_dict(self.data["rollback_plan"])
                              if self.data.get("rollback_plan") else None)
        self.stop_requested = False

    @staticmethod
    def _load_json(path: str) -> dict:
        with open(path, "r", encoding="utf-8") as fh:
            return json.load(fh)

    def reload(self) -> dict:
        self.data = self.store.read()
        return self.data

    # ---- 对单台服务的操作 -------------------------------------------------

    def service_status(self, node: str) -> dict:
        _, body = http_json(self.registry[node] + "/status")
        return body

    def apply_node(self, node: str, rules: Ruleset, epoch: int) -> dict:
        code, body = http_json(self.registry[node] + "/apply",
                               {"rules": ruleset_to_plain(rules), "epoch": epoch})
        if code == 409 and body.get("stale"):
            return body
        if code != 200 or "version" not in body:
            raise RuntimeError(f"{node} apply 返回 {code}: {body}")
        return body

    def fence_node(self, node: str, epoch: int) -> dict:
        code, body = http_json(self.registry[node] + "/fence", {"epoch": epoch})
        if code != 200 or "epoch_floor" not in body:
            raise RuntimeError(f"{node} fence 返回 {code}: {body}")
        return body

    def probe_node(self, node: str, cls: str) -> dict:
        return http_json(self.registry[node] + "/probe",
                         {"class": cls, "path": [], "visited": []})[1]

    def token_map(self, rules: Mapping[str, Ruleset]) -> Dict[str, str]:
        return {n: version_token(rules[n]) for n in self.topo.nodes}

    # ---- 实际版本对账 -----------------------------------------------------

    def reconcile(self) -> Tuple[Dict[str, str], List[str]]:
        """查询所有服务的实际生效版本, 返回 (节点->实际token, 不可用节点)。

        同时结算"已可见生效"的未决操作: 若某节点实际版本已等于该未决操作的
        目标版本, 说明确认虽丢失但操作确已生效(或迟到请求落地), 按实际结果
        收尾。未决条目只在实际可见或显式栅栏后才删除, 绝不凭猜测删除。
        """
        actual: Dict[str, str] = {}
        unreachable: List[str] = []
        max_floor = 0
        for node in self.topo.nodes:
            try:
                st = self.service_status(node)
                actual[node] = st["version"]
                max_floor = max(max_floor, int(st.get("epoch_floor", 0)))
            except RuntimeError as exc:
                unreachable.append(f"{node}({exc})")
        self.max_floor = max_floor
        if actual:
            def fn(data):
                pend = data.setdefault("pending", {})
                for node, tok in actual.items():
                    entry = pend.get(node)
                    if entry is not None and tok == entry.get("target"):
                        data["applied"][node] = tok
                        del pend[node]
                    elif data["applied"].get(node) != tok:
                        data["applied"][node] = tok
            self.store.mutate(fn)
            self.data = self.store.read()
        return actual, unreachable

    def _min_epoch(self) -> int:
        """分配新代数时必须超过服务端已有的最大栅栏(否则被旧栅栏拒止)。"""
        return int(getattr(self, "max_floor", 0))

    def pending(self) -> Dict[str, dict]:
        return dict(self.data.get("pending", {}))

    def _settled(self, tokens: Mapping[str, str],
                 nodes: Optional[Sequence[str]] = None) -> bool:
        """这些节点都已到目标版本, 且没有任何未决操作还挂在它们身上。"""
        data = self.store.read()
        applied = data["applied"]
        pend = data.get("pending", {})
        ns = list(nodes) if nodes is not None else list(self.topo.nodes)
        return (all(applied.get(n) == tokens[n] for n in ns)
                and not any(n in pend for n in ns))

    # ---- 单步推进 ---------------------------------------------------------

    def tick(self) -> str:
        """推进一次。返回当前 phase。"""
        self.reload()
        actual, unreachable = self.reconcile()
        phase = self.data["phase"]
        if unreachable:
            self.store.update(
                last_error=f"{now_str()} 以下服务不可达: {'; '.join(unreachable)}")
            self.data = self.store.read()
            return phase  # 不动配置, 下个 tick 重试; 连续不可达由 run 循环计数

        old_tok = self.token_map(self.old_rules)
        new_tok = self.token_map(self.new_rules)

        if phase in ("FORWARD_COMPLETE", "ROLLED_BACK"):
            return phase
        if phase == "PAUSED":
            # 暂停已接受: run 进程不推进、不开新批; 在途操作由 pause 命令结算。
            return "PAUSED"
        if self.data["direction"] == "rollback":
            return self._tick_rollback(actual, old_tok, new_tok, phase)
        return self._tick_forward(actual, old_tok, new_tok, phase)

    def _finish(self, phase: str) -> str:
        self.store.update(phase=phase, last_error=None)
        self.data = self.store.read()
        return phase

    def _tick_forward(self, actual, old_tok, new_tok, phase) -> str:
        plan = self.forward_plan.batches
        idx = self.data["current_batch_index"]

        if idx >= len(plan) or not plan:
            if self._settled(new_tok):
                return self._finish("FORWARD_COMPLETE")
            # 所有批次都发过但版本不齐: 继续对账补齐(典型的确认丢失场景)
            self._drive_batch(set(self.topo.nodes), old_tok, new_tok, batch=())
            if self._settled(new_tok):
                return self._finish("FORWARD_COMPLETE")
            return phase

        current = set(plan[idx])
        # 先确认之前的批次都已整体生效(且无未决操作), 再处理当前批
        prev_nodes = [n for i in range(idx) for n in plan[i]]
        prev_done = self._settled(new_tok, prev_nodes)
        if not prev_done:
            # 进程重启后的缝隙: 把欠账批次补齐, 仍不开新批
            for i in range(idx):
                self._drive_batch(set(plan[i]), old_tok, new_tok,
                                  batch=plan[i])
            return phase

        self._drive_batch(current, old_tok, new_tok, batch=plan[idx])
        if self._settled(new_tok, current):
            self.store.update(current_batch_index=idx + 1)
            self.data = self.store.read()
            if idx + 1 == len(plan) and self._settled(new_tok):
                return self._finish("FORWARD_COMPLETE")
        return self.store.read()["phase"]

    def _tick_rollback(self, actual, old_tok, new_tok, phase) -> str:
        assert self.rollback_plan is not None
        plan = self.rollback_plan.batches
        idx = self.data["current_batch_index"]

        if idx >= len(plan) or not plan:
            if self._settled(old_tok):
                return self._finish("ROLLED_BACK")
            self._drive_batch(set(self.topo.nodes), old_tok, new_tok, batch=())
            if self._settled(old_tok):
                return self._finish("ROLLED_BACK")
            return "ROLLING_BACK"

        current = set(plan[idx])
        prev_nodes = [n for i in range(idx) for n in plan[i]]
        if not self._settled(old_tok, prev_nodes):
            for i in range(idx):
                self._drive_batch(set(plan[i]), old_tok, new_tok,
                                  batch=plan[i])
            return "ROLLING_BACK"

        self._drive_batch(current, old_tok, new_tok, batch=plan[idx])
        if self._settled(old_tok, current):
            self.store.update(current_batch_index=idx + 1)
            self.data = self.store.read()
            if idx + 1 == len(plan) and self._settled(old_tok):
                return self._finish("ROLLED_BACK")
        return "ROLLING_BACK"

    def _drive_allowed(self) -> bool:
        return (self.data["phase"] in ("RUNNING", "ROLLING_BACK", "FAILED"))

    def _drive_batch(self, nodes: set, old_tok, new_tok, batch=()) -> bool:
        """把一批服务推进到各自目标版本; 已在目标版本的跳过。

        返回 True 表示因暂停已接受而拒绝继续下发(阻塞)。暂停后不会再发任何
        新操作, 因此本函数不会在暂停期间开启新批次。
        """
        direction = self.data["direction"]
        rules = self.new_rules if direction == "forward" else self.old_rules
        target_tok = new_tok if direction == "forward" else old_tok
        blocked = False
        for node in sorted(nodes):
            self.reload()
            if not self._drive_allowed() or self.data["direction"] != direction:
                blocked = True
                break
            target = target_tok[node]
            live = self.service_status(node)["version"]
            if live == target:
                if node in self.data.get("pending", {}):
                    print(f"  [{now_str()}] {node}: 实际已是目标版本"
                          f"(确认丢失), 按实际结果收尾", flush=True)
                self.store.observe(node, live)
                self.data = self.store.read()
                continue

            # 在发 HTTP 之前登记 pending 并分配 epoch: 崩溃也能接手, 旧请求必被压过
            epoch = self.store.begin_op(
                node, direction=direction, batch=list(batch), target=target,
                started_at=now_str(), min_epoch=self._min_epoch())
            self.data = self.store.read()
            if epoch is None:
                blocked = True
                break

            resp = None
            for attempt in range(APPLY_MAX_ATTEMPTS):
                try:
                    resp = self.apply_node(node, rules[node], epoch)
                    break
                except RuntimeError as exc:
                    print(f"  [{now_str()}] {node}: apply 第 {attempt + 1} 次"
                          f"失败(结果未知, 保留未决): {exc}", flush=True)
                    time.sleep(POLL_INTERVAL)
                    if not self._drive_allowed():
                        blocked = True
                        break
                    # 失败也可能其实已生效(确认丢失): 查真实状态
                    try:
                        live = self.service_status(node)["version"]
                    except RuntimeError:
                        live = None
                    if live == target:
                        resp = {"version": live, "duplicate": True}
                        break
            if blocked:
                break
            if resp is None:
                # 本节点暂时推不动: 抛出由 run 循环计数, 下个 tick 再试, 不开新批
                raise RuntimeError(f"{node} 连续 {APPLY_MAX_ATTEMPTS} 次 apply 未确认")

            if resp.get("stale"):
                # 被更高代数的栅栏压过(暂停/回退): 绝不覆盖, 以实际状态收尾
                live = self.service_status(node)["version"]
                self.store.finish_op(node, epoch, effective=(live == target),
                                     version=live)
                self.data = self.store.read()
                print(f"  [{now_str()}] {node}: 旧代数请求被栅栏拒绝, "
                      f"实际版本 {short(live)}", flush=True)
                continue
            if resp.get("duplicate"):
                mark = "重复请求, 服务幂等忽略"
            elif "changed" in resp:
                mark = "已切换"
            else:
                mark = "对账确认生效"
            self.store.finish_op(node, epoch, effective=True,
                                 version=resp["version"])
            self.data = self.store.read()
            print(f"  [{now_str()}] {node}: apply -> {resp['version']} ({mark})",
                  flush=True)
        return blocked

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

    # ---- 在途操作结算(暂停/回退前) ---------------------------------------

    def settle_pending(self, epoch: int, timeout: float = 30.0,
                       verbose: bool = True) -> bool:
        """把在途操作结算到"确定结果": 返回 True 表示全部已明确。

        做法: 对所有节点装栅栏(epoch 由调用方保证严格大于任何在途操作的
        代数)。栅栏落地后, 任何更早代数的 apply 都不可能再生效; 因此此后
        查到的实际版本就是最终结果——生效就记为生效, 没生效就记为不生效。
        """
        fenced: set = set()
        deadline = time.monotonic() + timeout
        while True:
            unreachable: List[str] = []
            need_fence = [n for n in self.topo.nodes if n not in fenced]
            for node in list(need_fence):
                try:
                    self.fence_node(node, epoch)
                    fenced.add(node)
                except RuntimeError as exc:
                    unreachable.append(f"{node}(fence: {exc})")
            # 栅栏装好(至少该节点)后再查实际版本, 才是不会被改写的最终结果
            actual, unreach2 = self.reconcile()
            unreachable.extend(unreach2)
            pend = self.pending()
            for node in list(pend):
                if node not in fenced:
                    continue
                entry = pend[node]
                tok = actual.get(node)
                if tok is None:
                    continue  # 不可达, 等
                self.store.finish_op(node, entry["epoch"],
                                     effective=(tok == entry["target"]),
                                     version=tok)
            self.data = self.store.read()
            pend = self.pending()

            if not unreachable and not pend and len(fenced) == len(self.topo.nodes):
                if verbose:
                    print(f"[{now_str()}] 在途操作均已明确(生效或已永久拒止)", flush=True)
                return True
            if time.monotonic() >= deadline:
                if verbose:
                    print(f"[{now_str()}] 等待超时: 仍有 {len(pend)} 个未决操作, "
                          f"不可达节点 {len(unreachable)} 个; 未决状态已保留, "
                          f"可用 status 查看", flush=True)
                    for node, entry in sorted(pend.items()):
                        print(f"    {node}: epoch={entry['epoch']} "
                              f"目标={short(entry['target'])} "
                              f"方向={entry['direction']} 起始 {entry['started_at']}",
                              flush=True)
                return False
            time.sleep(POLL_INTERVAL)

    # ---- 回退计划 ---------------------------------------------------------

    def ensure_rollback_plan(self, max_batch_size: int,
                             max_batches: int) -> Plan:
        if self.rollback_plan is not None:
            return self.rollback_plan
        plan = self._plan_rollback(max_batch_size, max_batches)
        self.rollback_plan = plan
        return plan

    def _plan_rollback(self, max_batch_size: int, max_batches: int) -> Plan:
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
        self.store.update(rollback_plan=plan.to_dict())
        self.data = self.store.read()
        return plan

    def rollback_plan_unsafe(self, actual: Mapping[str, str],
                            old_tok: Mapping[str, str],
                            new_tok: Mapping[str, str]) -> Optional[str]:
        """校验既有回退计划在当前现场下仍满足安全约束; 返回原因或 None。

        现场必须是"规划起点某个已生效前缀": 即每台服务要么是计划的起点版本,
        要么是该节点所在批次的目标(老版本), 且整体混合状态可达。这覆盖了
        "回退中途某批已部分生效"和"迟到请求让个别节点回到目标"的情形。
        """
        from .model import mixed_assignment_reachable
        plan = self.rollback_plan
        if plan is None:
            return "无回退计划"
        cur_rules: Dict[str, Ruleset] = {}
        for node in self.topo.nodes:
            tok = actual[node]
            if tok == old_tok[node]:
                cur_rules[node] = self.old_rules[node]
            elif tok == new_tok[node]:
                cur_rules[node] = self.new_rules[node]
            else:
                return f"节点 {node} 实际版本 {short(tok)} 既非发布前也非目标版本"
            if tok != plan.from_tokens.get(node) and tok != old_tok[node]:
                return (f"节点 {node} 实际版本 {short(tok)} 不是计划起点也无批次覆盖"
                        f"(迟到请求已落地)")
        return mixed_assignment_reachable(self.topo, cur_rules, self.old_rules,
                                          frozenset())


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
    pub = Publisher(store)
    pub.reconcile()  # 观测服务端现有最大栅栏, 新代数必须超过它
    epoch, result = store.accept_pause(min_epoch=pub._min_epoch())
    if result == "not_applicable":
        print(f"当前阶段 {store.read()['phase']} 无需暂停")
        return
    if result == "already":
        print(f"[{now_str()}] 暂停此前已接受, 继续等待在途操作明确...")
    else:
        print(f"[{now_str()}] 已接受暂停: 所有进程不再开启新批次, "
              f"等待在途操作出确定结果...")
    ok = pub.settle_pending(int(epoch), timeout=args.timeout)
    if not ok:
        raise SystemExit(1)
    print(f"[{now_str()}] 暂停完成: 在途操作已明确, 进度已保存, 可 resume 或 rollback")


def cmd_rollback(args) -> None:
    store = ReleaseStore(args.state)
    data = store.read()
    if data["phase"] == "ROLLED_BACK":
        print("已经处于回退完成状态")
        return
    if data["phase"] not in ("FORWARD_COMPLETE", "PAUSED", "ROLLING_BACK"):
        raise SystemExit(
            f"当前阶段 {data['phase']} 不能回退: 只在正向发布或暂停完成后才规划回退。"
            f"请先 resume 至完成, 或先 pause 让在途操作确定。")
    pub = Publisher(store)

    # 回退前先装上更高的栅栏, 并让在途操作有确定结果。栅栏必须无条件装到每个
    # 节点: 否则"本就已是老版本、不需要 apply"的节点没有更大代数, 迟到的正向
    # 请求仍能把它翻回新配置。超时则拒绝回退(不拿不确定的现场做规划)。
    pub.reconcile()  # 先观测服务端现有最大栅栏, 新代数必须超过它
    epoch = store.alloc_epoch(min_epoch=pub._min_epoch())
    print(f"[{now_str()}] 回退前装栅栏并结算在途操作 (epoch {epoch})...")
    if not pub.settle_pending(epoch, timeout=args.timeout):
        raise SystemExit("存在未决操作未能确定, 拒绝回退; 状态已保留")

    actual, unreachable = pub.reconcile()
    if unreachable:
        raise SystemExit(f"存在不可达服务, 无法安全规划回退: {'; '.join(unreachable)}")
    old_tok = pub.token_map(pub.old_rules)
    new_tok = pub.token_map(pub.new_rules)

    if data["phase"] == "ROLLING_BACK" and pub.rollback_plan is not None:
        reason = pub.rollback_plan_unsafe(actual, old_tok, new_tok)
        if reason:
            print(f"[{now_str()}] 现场与既有回退计划不符({reason}), 按实际重新规划")
            store.update(rollback_plan=None, current_batch_index=0)
            pub.rollback_plan = None
        else:
            print("回退已在进行, 直接续跑(保留批次进度)")
    if pub.rollback_plan is None:
        plan = pub.ensure_rollback_plan(args.max_batch_size, args.max_batches)
        store.update(phase="ROLLING_BACK", direction="rollback",
                     current_batch_index=0, rollback_plan=plan.to_dict())
        pub.data = store.read()

    plan = pub.rollback_plan
    print("回退计划(以各节点实际生效规则为起点, 同样受单批规模与可达性约束):")
    for i, batch in enumerate(plan.batches, 1):
        print(f"  回退批 {i:>2}: {', '.join(batch)}")
    store.update(phase="ROLLING_BACK", direction="rollback")
    pub.data = store.read()
    _install_signals(pub)
    phase = pub.run(max_seconds=args.max_seconds)
    print(f"[{now_str()}] rollback 结束, 当前阶段: {phase}")
    if phase not in ("ROLLED_BACK", "PAUSED"):
        raise SystemExit(1)


def cmd_status(args) -> None:
    store = ReleaseStore(args.state)
    data = store.read()
    pub = Publisher(store)
    old_tok = pub.token_map(pub.old_rules)
    new_tok = pub.token_map(pub.new_rules)
    print("=" * 68)
    print(f"发布 {data['release_id']}  阶段: {data['phase']}  "
          f"方向: {data['direction']}  栅栏代数: {data.get('epoch')}")
    print("=" * 68)
    plan = (pub.rollback_plan if data["direction"] == "rollback"
            and pub.rollback_plan else pub.forward_plan)
    for i, batch in enumerate(plan.batches, 1):
        print(f"  批次 {i:>2}: {', '.join(batch)}")
    print("-" * 68)
    print(f"{'服务':<10}{'实际版本':<22}{'发布前':<10}{'目标':<10}状态")
    actual, unreachable = pub.reconcile()
    pub.data = store.read()
    for node in pub.topo.nodes:
        tok = actual.get(node, "<不可达>")
        if tok == new_tok[node]:
            mark = "新配置已生效"
        elif tok == old_tok[node]:
            mark = "发布前配置"
        else:
            mark = "未知版本"
        print(f"{node:<10}{short(tok, 18):<22}{short(old_tok[node]):<10}"
              f"{short(new_tok[node]):<10}{mark}")
    if unreachable:
        print("不可达: " + "; ".join(unreachable))
    pend = store.read().get("pending", {})
    if pend:
        print("未决操作(已发出未确认):")
        for node, entry in sorted(pend.items()):
            print(f"  {node}: epoch={entry['epoch']} 目标={short(entry['target'], 18)} "
                  f"方向={entry['direction']} 批次={entry.get('batch')} "
                  f"自 {entry['started_at']}")
    else:
        print("未决操作: 无")
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

    p = sub.add_parser("pause", help="暂停: 不开新批, 等在途操作确定并保存进度")
    p.add_argument("--state", required=True)
    p.add_argument("--timeout", type=float, default=30)
    p.set_defaults(func=cmd_pause)

    p = sub.add_parser("rollback", help="回退到发布前配置(重新做受约束规划)")
    p.add_argument("--state", required=True)
    p.add_argument("--max-batch-size", type=int, default=0)
    p.add_argument("--max-batches", type=int, default=0)
    p.add_argument("--max-seconds", type=float, default=300)
    p.add_argument("--timeout", type=float, default=30)
    p.set_defaults(func=cmd_rollback)

    p = sub.add_parser("status", help="查询发布进度、未决操作与各服务实际生效版本")
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

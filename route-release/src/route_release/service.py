"""可独立运行的本地转发服务(支持固定出口迁移期间的粘性路由)。

每台服务持有本节点当前生效的整套下一跳规则, 并按"发布版本标签"(old/new)
保留另一套规则集。控制面提供:
  GET  /status        查询实际生效版本、保留版本、停用项与在途计数
  POST /apply         整套切换本机规则(幂等; 可带 label/previous_label,
                      切换时按标签保留上一版本规则集, 供粘性选路)
  POST /probe         从本节点出发的真实逐跳转发; 支持 pin(版本标签):
                      入口按入站时本节点所处版本选定 pin, 之后逐跳透传,
                      每个节点按 pin 选当前或保留版本的规则, 保证一条请求
                      从入口到固定出口始终走同一套规则
  POST /decommission  停用本节点作为某类流量某版本标签的接收(前置闸校验)
  POST /recommission  恢复接收
  POST /fixture       本地夹具: 给指定类别的 probe 注入延迟(仅内存, 重启失效)
  POST /shutdown      仅用于本地测试/演示的收尾

生效配置、保留规则集与停用项通过"临时文件 + os.replace"原子落盘;
进程重启后自动恢复。在途计数是运行态信息, 只存在内存里。
仅使用标准库, 可用 `python -I` 直接运行。
"""
from __future__ import annotations

import argparse
import json
import os
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Dict, List, Optional

from .model import Rule, Ruleset, ruleset_from_plain, ruleset_to_plain, version_token

STATE_FORMAT = 3
BASELINE_LABEL = "old"


class ForwardingState:
    """一台服务的内存状态 + 磁盘持久化(按版本标签保留多套规则)。"""

    def __init__(self, node: str, state_path: str, registry_path: str,
                 initial: Ruleset):
        self.node = node
        self.state_path = state_path
        self.registry_path = registry_path
        self.lock = threading.Lock()
        self.rules: Ruleset = {}
        self.version: str = ""
        # 版本标签 -> 规则集(迁移窗口内同时保留 old/new 两套)
        self.rulesets: Dict[str, Ruleset] = {}
        # 版本标签 -> 该标签规则集的内容指纹
        self.version_by_label: Dict[str, str] = {}
        self.current_label: Optional[str] = None
        # 已停用接收的 (流量类别, 版本标签) 列表
        self.decommissioned: List[dict] = []
        # 在途计数(只在内存): 入口请求计数 / 到达本节点的请求计数, 按标签
        self.inflight: Dict[str, int] = {}
        self.visiting: Dict[str, int] = {}
        # 本地夹具: 这些类别的 probe 在本节点延迟一次
        self.fault = {"slow_classes": [], "delay_sec": 0.0, "remaining": 0}
        os.makedirs(os.path.dirname(os.path.abspath(state_path)), exist_ok=True)
        loaded = self._load()
        if loaded is not None:
            (self.rules, self.version, self.rulesets, self.version_by_label,
             self.current_label, self.decommissioned) = loaded
        else:
            self.rules = dict(initial)
            self.version = version_token(self.rules)
            self._persist_locked()

    def _load(self) -> Optional[tuple]:
        if not os.path.exists(self.state_path):
            return None
        with open(self.state_path, "r", encoding="utf-8") as fh:
            data = json.load(fh)
        if data.get("node") != self.node:
            raise RuntimeError(f"状态文件属于节点 {data.get('node')}, 与 {self.node} 不符")
        rules = ruleset_from_plain(data["rules"])
        rulesets = {label: ruleset_from_plain(plain)
                    for label, plain in data.get("rulesets", {}).items()}
        version_by_label = dict(data.get("version_by_label", {}))
        decom = list(data.get("decommissioned", []))
        return (rules, data["version"], rulesets, version_by_label,
                data.get("current_label"), decom)

    def _persist_locked(self) -> None:
        payload = {
            "format": STATE_FORMAT,
            "node": self.node,
            "version": self.version,
            "rules": ruleset_to_plain(self.rules),
            "rulesets": {label: ruleset_to_plain(rs)
                         for label, rs in sorted(self.rulesets.items())},
            "version_by_label": self.version_by_label,
            "current_label": self.current_label,
            "decommissioned": self.decommissioned,
        }
        tmp = self.state_path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump(payload, fh, ensure_ascii=False, indent=2)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, self.state_path)

    def status(self) -> dict:
        with self.lock:
            return {
                "node": self.node,
                "version": self.version,
                "current_label": self.current_label,
                "rules": ruleset_to_plain(self.rules),
                "labels": sorted(self.rulesets),
                "version_by_label": dict(self.version_by_label),
                "decommissioned": list(self.decommissioned),
                "inflight": dict(self.inflight),
                "visiting": dict(self.visiting),
                "next_hops": {
                    cls: ("<local-egress>" if r.local_egress else r.next_hop)
                    for cls, r in sorted(self.rules.items())
                },
            }

    def apply(self, plain_rules: Dict[str, dict],
              label: Optional[str] = None,
              previous_label: Optional[str] = None) -> dict:
        """整套切换; 内容相同则幂等返回。

        带 label 时按发布版本标签管理: 切换前把当前规则集登记到
        previous_label(默认 old), 新规则登记到 label, 这样粘性请求在切换
        窗口内始终能按标签找到对应版本的规则。
        """
        new_rules = ruleset_from_plain(plain_rules)
        new_version = version_token(new_rules)
        with self.lock:
            if new_version == self.version:
                if label:
                    # 幂等分支也要保证两套标签都登记并持久化: 新旧规则内容
                    # 相同的节点(本节点规则未变)同样要能按 old/new 粘性选路,
                    # 且进程重启后两套规则集都还在。
                    self.version_by_label.setdefault(label, new_version)
                    self.rulesets.setdefault(label, dict(new_rules))
                    prev = previous_label or BASELINE_LABEL
                    self.version_by_label.setdefault(prev, new_version)
                    self.rulesets.setdefault(prev, dict(new_rules))
                    if self.current_label is None:
                        self.current_label = label
                    self._persist_locked()
                return {"node": self.node, "version": self.version,
                        "changed": False, "duplicate": True,
                        "current_label": self.current_label,
                        "labels": sorted(self.rulesets)}
            old_version = self.version
            old_rules = self.rules
            if label:
                prev = previous_label or BASELINE_LABEL
                # 按标签保留旧规则集: 粘性请求在切换窗口内仍按 pin 找到它
                self.rulesets[prev] = dict(old_rules)
                self.version_by_label[prev] = old_version
                self.rulesets[label] = dict(new_rules)
                self.version_by_label[label] = new_version
                self.current_label = label
            self.rules = new_rules
            self.version = new_version
            self._persist_locked()
            return {"node": self.node, "version": self.version,
                    "old_version": old_version, "changed": True,
                    "duplicate": False, "current_label": self.current_label,
                    "labels": sorted(self.rulesets)}

    def decommission(self, cls: str, label: str) -> dict:
        """停用本节点对 (cls, label) 规则的接收; 前置闸不过则拒绝且不改状态。"""
        with self.lock:
            entry = {"class": cls, "label": label}
            if entry in self.decommissioned:
                return {"node": self.node, "ok": True, "duplicate": True}
            # 闸 1: 当前生效规则已不再本地吸收该类别(入口全切新后才成立)
            if cls in self.rules and self.rules[cls].local_egress:
                return {"node": self.node, "ok": False,
                        "error": f"当前生效规则仍在本地吸收类别 {cls}, "
                                 f"不能停用旧出口接收"}
            # 闸 2: 没有在途请求(无论入口还是转发到达)仍依赖这个版本标签
            if self.inflight.get(label, 0) or self.visiting.get(label, 0):
                return {"node": self.node, "ok": False,
                        "error": f"仍有在途请求依赖版本 {label} "
                                 f"(inflight={self.inflight.get(label, 0)}, "
                                 f"visiting={self.visiting.get(label, 0)}), "
                                 f"拒绝停用"}
            self.decommissioned.append(entry)
            self._persist_locked()
            return {"node": self.node, "ok": True,
                    "decommissioned": self.decommissioned}

    def recommission(self, cls: str, label: str) -> dict:
        """恢复接收(回退第一步, 幂等)。"""
        with self.lock:
            entry = {"class": cls, "label": label}
            if entry in self.decommissioned:
                self.decommissioned.remove(entry)
                self._persist_locked()
                return {"node": self.node, "ok": True, "changed": True}
            return {"node": self.node, "ok": True, "changed": False}

    def set_fixture(self, slow_classes: List[str], delay_sec: float,
                    remaining: int) -> dict:
        with self.lock:
            self.fault = {"slow_classes": list(slow_classes),
                          "delay_sec": float(delay_sec),
                          "remaining": int(remaining)}
            return {"node": self.node, "fixture": self.fault}

    def _registry(self) -> dict:
        with open(self.registry_path, "r", encoding="utf-8") as fh:
            return json.load(fh)

    def probe(self, cls: str, path_so_far: list, visited: set,
              pin: Optional[str]) -> dict:
        """粘性逐跳转发: 入口选定 pin(版本标签), 每跳按该标签的规则走。"""
        import time
        import urllib.request

        is_entry = not path_so_far
        with self.lock:
            if cls not in self.rules:
                return {"ok": False, "path": path_so_far + [self.node],
                        "error": f"节点 {self.node} 没有类别 {cls} 的规则"}
            if is_entry and pin is None:
                # 入口: 按入站时本节点所处版本选定 pin, 整条请求粘住它。
                # 首次启动未登记标签时, 基线配置视作 old。
                pin = self.current_label or BASELINE_LABEL
            active_pin = pin
            if is_entry:
                self.inflight[active_pin] = self.inflight.get(active_pin, 0) + 1
            self.visiting[active_pin] = self.visiting.get(active_pin, 0) + 1
            # 选路: pin 等于当前标签用现行规则; 该标签有保留规则就用保留规则;
            # 标签未登记(普通发布/无迁移的节点)则回退到现行规则, 保证不中断。
            if active_pin == self.current_label or active_pin not in self.rulesets:
                rule = self.rules[cls]
            else:
                rule = self.rulesets[active_pin][cls]
            fault = dict(self.fault)

        try:
            # 本地夹具: 让一条老请求在迁移窗口内停在某一跳, 跨切换后再继续
            if cls in fault["slow_classes"] and fault["remaining"] > 0:
                with self.lock:
                    if self.fault["remaining"] > 0:
                        self.fault["remaining"] -= 1
                        delay = self.fault["delay_sec"]
                    else:
                        delay = 0.0
                if delay:
                    time.sleep(delay)

            if self.node in visited:
                return {"ok": False, "path": path_so_far + [self.node],
                        "error": f"转发环路, 重复经过 {self.node}"}
            visited = visited | {self.node}
            path = path_so_far + [self.node]

            with self.lock:
                decom_match = any(d["class"] == cls and d["label"] == active_pin
                                  for d in self.decommissioned)
            if decom_match:
                return {"ok": False, "path": path,
                        "error": f"节点 {self.node} 已停用类别 {cls} 版本 "
                                 f"{active_pin} 的接收(旧出口已下线)"}
            if rule.local_egress:
                return {"ok": True, "path": path, "egress": self.node,
                        "pin": active_pin,
                        "observed_versions": {self.node: self.version}}

            registry = self._registry()
            if rule.next_hop not in registry:
                return {"ok": False, "path": path,
                        "error": f"下一跳 {rule.next_hop} 未在注册表中"}
            body = json.dumps({"class": cls, "path": path,
                               "visited": sorted(visited),
                               "pin": active_pin}).encode("utf-8")
            req = urllib.request.Request(
                registry[rule.next_hop] + "/probe", data=body,
                headers={"Content-Type": "application/json"}, method="POST")
            try:
                with urllib.request.urlopen(req, timeout=10) as resp:
                    downstream = json.loads(resp.read().decode("utf-8"))
            except Exception as exc:
                return {"ok": False, "path": path,
                        "error": f"访问下一跳 {rule.next_hop} 失败: {exc}"}
            versions = downstream.get("observed_versions", {})
            versions[self.node] = self.version
            downstream["observed_versions"] = versions
            downstream["pin"] = active_pin
            return downstream
        finally:
            with self.lock:
                self._release_counters_locked(is_entry, active_pin)

    def _release_counters_locked(self, is_entry: bool, pin: str) -> None:
        if is_entry:
            self.inflight[pin] = max(0, self.inflight.get(pin, 0) - 1)
        self.visiting[pin] = max(0, self.visiting.get(pin, 0) - 1)


class Handler(BaseHTTPRequestHandler):
    state: ForwardingState = None  # type: ignore[assignment]

    def log_message(self, fmt, *args):  # 安静一点, 演示时由测试框架统一输出
        pass

    def _send(self, code: int, obj: dict) -> None:
        body = json.dumps(obj, ensure_ascii=False).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _read_json(self) -> dict:
        length = int(self.headers.get("Content-Length", "0"))
        return json.loads(self.rfile.read(length).decode("utf-8"))

    def do_GET(self):
        if self.path == "/status":
            self._send(200, self.state.status())
        else:
            self._send(404, {"error": "unknown path"})

    def do_POST(self):
        try:
            data = self._read_json()
            if self.path == "/apply":
                if "rules" not in data:
                    self._send(400, {"error": "缺少 rules 字段"})
                    return
                self._send(200, self.state.apply(
                    data["rules"], data.get("label"),
                    data.get("previous_label")))
            elif self.path == "/probe":
                result = self.state.probe(
                    data["class"], data.get("path", []),
                    set(data.get("visited", [])), data.get("pin"))
                self._send(200 if result["ok"] else 409, result)
            elif self.path == "/decommission":
                label = data.get("label", data.get("token"))
                result = self.state.decommission(data["class"], label)
                self._send(200 if result.get("ok") else 409, result)
            elif self.path == "/recommission":
                label = data.get("label", data.get("token"))
                self._send(200, self.state.recommission(
                    data["class"], label))
            elif self.path == "/fixture":
                self._send(200, self.state.set_fixture(
                    data.get("slow_classes", []),
                    float(data.get("delay_sec", 0)),
                    int(data.get("remaining", 0))))
            elif self.path == "/shutdown":
                self._send(200, {"ok": True})
                threading.Thread(target=self.server.shutdown, daemon=True).start()
            else:
                self._send(404, {"error": "unknown path"})
        except Exception as exc:
            self._send(500, {"error": f"{type(exc).__name__}: {exc}"})


def run_server(node: str, port: int, topology_path: str, registry_path: str,
               state_dir: str, initial_rules_path: Optional[str]) -> None:
    with open(topology_path, "r", encoding="utf-8") as fh:
        topo = json.load(fh)
    if node not in topo["nodes"]:
        raise SystemExit(f"拓扑中不存在节点 {node}")
    initial: Ruleset = {}
    if initial_rules_path:
        with open(initial_rules_path, "r", encoding="utf-8") as fh:
            raw = json.load(fh)
        initial = ruleset_from_plain(raw[node])
    state = ForwardingState(
        node, os.path.join(state_dir, f"{node}.json"), registry_path, initial)
    server = ThreadingHTTPServer(("127.0.0.1", port), Handler)
    Handler.state = state
    print(f"[service {node}] listening on 127.0.0.1:{port}, "
          f"effective version {state.version}", flush=True)
    try:
        server.serve_forever()
    finally:
        server.server_close()


def main(argv=None) -> None:
    ap = argparse.ArgumentParser(description="本地转发服务")
    ap.add_argument("--node", required=True)
    ap.add_argument("--port", type=int, required=True)
    ap.add_argument("--topology", required=True)
    ap.add_argument("--registry", required=True)
    ap.add_argument("--state-dir", required=True)
    ap.add_argument("--initial-rules", default=None,
                    help="首次启动(无状态文件)时加载的整套规则文件")
    args = ap.parse_args(argv)
    run_server(args.node, args.port, args.topology, args.registry,
               args.state_dir, args.initial_rules)


if __name__ == "__main__":
    main()

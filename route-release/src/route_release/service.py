"""可独立运行的本地转发服务。

每台服务持有本节点的整套下一跳规则, 控制面提供:
  GET  /status   查询实际生效版本、栅栏代数与各类流量下一跳
  POST /apply    整套切换本机规则(幂等: 按规则内容指纹去重, 确认丢失可安全重发)
  POST /fence    抬高栅栏代数, 让更早代数的 apply 永久失效(暂停/回退的确定性来源)
  POST /probe    从本节点出发模拟某类流量的真实转发, 逐跳访问下游服务,
                 带环路检测, 返回完整路径
  POST /shutdown 仅用于本地测试/演示的收尾

栅栏代数(epoch floor): 每次 apply 可携带一个单调递增的 epoch。服务记住见过的
最大值; 任何 epoch 小于该值的 apply 一律 409 拒绝且不改配置。因此暂停/回退一旦
装上更高的栅栏, 迟到的正向请求(或重发)再到达也会被拒——确认丢失、网络延迟都
不会让旧意图覆盖新结果。

生效配置通过"临时文件 + os.replace"原子落盘; 进程重启后自动恢复(含栅栏代数)。
仅使用标准库, 可用 `python -I` 直接运行。
"""
from __future__ import annotations

import argparse
import json
import os
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Dict, Optional

from .model import Rule, Ruleset, ruleset_from_plain, ruleset_to_plain, version_token


class ForwardingState:
    """一台服务的内存状态 + 磁盘持久化。"""

    def __init__(self, node: str, state_path: str, registry_path: str,
                 initial: Ruleset):
        self.node = node
        self.state_path = state_path
        self.registry_path = registry_path
        self.lock = threading.Lock()
        self.rules: Ruleset = {}
        self.version: str = ""
        self.epoch_floor: int = 0
        os.makedirs(os.path.dirname(os.path.abspath(state_path)), exist_ok=True)
        loaded = self._load()
        if loaded is not None:
            self.rules, self.version, self.epoch_floor = loaded
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
        return rules, data["version"], int(data.get("epoch_floor", 0))

    def _persist_locked(self) -> None:
        payload = {
            "node": self.node,
            "version": self.version,
            "epoch_floor": self.epoch_floor,
            "rules": ruleset_to_plain(self.rules),
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
                "epoch_floor": self.epoch_floor,
                "rules": ruleset_to_plain(self.rules),
                "next_hops": {
                    cls: ("<local-egress>" if r.local_egress else r.next_hop)
                    for cls, r in sorted(self.rules.items())
                },
            }

    def apply(self, plain_rules: Dict[str, dict], epoch: Optional[int] = None) -> dict:
        """整套切换; 内容相同则直接返回当前版本(幂等)。

        带 epoch 时: 低于当前栅栏的请求被拒(不改配置, 返回 stale);
        不小于栅栏则抬高栅栏并生效。不带 epoch 的请求保持旧行为(测试/手工)。
        """
        new_rules = ruleset_from_plain(plain_rules)
        new_version = version_token(new_rules)
        with self.lock:
            if epoch is not None:
                epoch = int(epoch)
                if epoch < self.epoch_floor:
                    return {"node": self.node, "version": self.version,
                            "epoch_floor": self.epoch_floor, "stale": True,
                            "changed": False, "duplicate": False}
                self.epoch_floor = epoch
            if new_version == self.version:
                self._persist_locked()  # 栅栏可能被抬高, 需要落盘
                return {"node": self.node, "version": self.version,
                        "epoch_floor": self.epoch_floor,
                        "changed": False, "duplicate": True}
            old_version = self.version
            self.rules = new_rules
            self.version = new_version
            self._persist_locked()
            return {"node": self.node, "version": self.version,
                    "epoch_floor": self.epoch_floor,
                    "old_version": old_version, "changed": True,
                    "duplicate": False}

    def fence(self, epoch: int) -> dict:
        """抬高栅栏代数; 不改规则。低于该代数的 apply 从此被拒。"""
        with self.lock:
            if epoch > self.epoch_floor:
                self.epoch_floor = epoch
                self._persist_locked()
            return {"node": self.node, "version": self.version,
                    "epoch_floor": self.epoch_floor}

    def _registry(self) -> dict:
        with open(self.registry_path, "r", encoding="utf-8") as fh:
            return json.load(fh)

    def probe(self, cls: str, path_so_far: list, visited: set) -> dict:
        """模拟真实转发: 本节点命中规则后, 再逐跳请求下游服务。"""
        with self.lock:
            if cls not in self.rules:
                return {"ok": False, "path": path_so_far + [self.node],
                        "error": f"节点 {self.node} 没有类别 {cls} 的规则"}
            rule = self.rules[cls]
            current_version = self.version
        if self.node in visited:
            return {"ok": False, "path": path_so_far + [self.node],
                    "error": f"转发环路, 重复经过 {self.node}"}
        visited = visited | {self.node}
        path = path_so_far + [self.node]
        if rule.local_egress:
            return {"ok": True, "path": path, "egress": self.node,
                    "observed_versions": {self.node: current_version}}
        import urllib.request

        registry = self._registry()
        if rule.next_hop not in registry:
            return {"ok": False, "path": path,
                    "error": f"下一跳 {rule.next_hop} 未在注册表中"}
        body = json.dumps({"class": cls, "path": path,
                           "visited": sorted(visited)}).encode("utf-8")
        req = urllib.request.Request(
            registry[rule.next_hop] + "/probe", data=body,
            headers={"Content-Type": "application/json"}, method="POST")
        try:
            with urllib.request.urlopen(req, timeout=5) as resp:
                downstream = json.loads(resp.read().decode("utf-8"))
        except Exception as exc:  # 下游不可达也是值班人员需要看到的故障
            return {"ok": False, "path": path,
                    "error": f"访问下一跳 {rule.next_hop} 失败: {exc}"}
        versions = downstream.get("observed_versions", {})
        versions[self.node] = current_version
        downstream["observed_versions"] = versions
        return downstream


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
                result = self.state.apply(data["rules"], data.get("epoch"))
                self._send(409 if result.get("stale") else 200, result)
            elif self.path == "/fence":
                if "epoch" not in data:
                    self._send(400, {"error": "缺少 epoch 字段"})
                    return
                self._send(200, self.state.fence(int(data["epoch"])))
            elif self.path == "/probe":
                result = self.state.probe(
                    data["class"], data.get("path", []),
                    set(data.get("visited", [])))
                self._send(200 if result["ok"] else 409, result)
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

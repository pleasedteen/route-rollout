"""故障夹具代理: 夹在发布工具与转发服务之间, 复现发布现场故障。

支持的故障模式(写在 JSON 配置里):
  drop_confirm  服务已执行成功, 但代理吞掉响应 -> 发布进程收不到确认
  drop_request  代理直接丢弃请求(服务根本不知道这次切换)
  flaky         前几次请求失败, 之后恢复 -> 验证重试/幂等
  delay         响应延迟, 模拟慢节点

只代理 HTTP, 服务之间的 /probe 走注册表直连, 不经过本代理。
"""
from __future__ import annotations

import argparse
import json
import os
import random
import threading
import time
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


class ProxyConfig:
    def __init__(self, path: str):
        self.path = path
        self.lock = threading.Lock()
        self._mtime = -1.0
        self.reload(force=True)

    def reload(self, force: bool = False) -> None:
        try:
            mtime = os.path.getmtime(self.path)
        except OSError:
            return
        if not force and mtime == self._mtime:
            return
        with open(self.path, "r", encoding="utf-8") as fh:
            data = json.load(fh)
        with self.lock:
            new_sig = (data.get("mode", "pass"), int(data.get("fail_times", 1)))
            should_reset = force or not hasattr(self, "remaining") \
                or new_sig != self._signature()
            self.target = data["target"]
            self.mode = data.get("mode", "pass")
            self.fail_times = int(data.get("fail_times", 1))
            self.delay_sec = float(data.get("delay_sec", 0))
            self.seed = int(data.get("seed", 0))
            if should_reset:  # 模式/失败次数变化才重置计数, 便于在线切换故障
                self.rng = random.Random(self.seed)
                self.remaining = self.fail_times
                self.counters = {"requests": 0, "dropped_confirm": 0,
                                 "dropped_request": 0, "flaky_failed": 0,
                                 "passed": 0}
            self._mtime = mtime

    def _signature(self):
        return (self.mode, self.fail_times)

    def decide(self) -> str:
        """返回 drop_confirm / drop_request / flaky_fail / pass。

        所有故障模式都遵守 fail_times 配额: 注入这么多次后自动恢复为 pass,
        用来复现"前几次失败、之后自愈"的真实故障。
        """
        self.reload()  # 在线读取最新故障配置
        with self.lock:
            self.counters["requests"] += 1
            mode = self.mode
            if mode == "pass":
                action = "pass"
            elif mode in ("drop_confirm", "drop_request", "flaky"):
                if self.remaining > 0:
                    self.remaining -= 1
                    action = ("flaky_fail" if mode == "flaky" else mode)
                else:
                    action = "pass"
            else:
                raise ValueError(f"未知故障模式 {mode}")
            if action == "pass":
                self.counters["passed"] += 1
            else:
                self.counters[action] = self.counters.get(action, 0) + 1
            delay = self.delay_sec
        if delay:
            time.sleep(delay)
        return action


class Handler(BaseHTTPRequestHandler):
    cfg: ProxyConfig = None  # type: ignore[assignment]

    def log_message(self, fmt, *args):
        pass

    def _send(self, code, body: bytes, ctype="application/json; charset=utf-8"):
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _handle(self, method: str):
        length = int(self.headers.get("Content-Length", "0"))
        payload = self.rfile.read(length) if length else b""
        # 故障只作用于变更操作 POST /apply。状态查询 /status、逐跳探测 /probe
        # 始终放行——这正是"确认丢失但能查到真实生效状态"的前提。
        fault_active = (method == "POST" and self.path == "/apply")
        action = self.cfg.decide() if fault_active else "pass"
        if action == "drop_request":
            # 模拟请求在网络上消失: 直接关闭连接, 什么都不转发
            self.close_connection = True
            return
        url = self.cfg.target + self.path
        req = urllib.request.Request(url, data=payload if method == "POST" else None,
                                     method=method)
        req.add_header("Content-Type",
                       self.headers.get("Content-Type", "application/json"))
        try:
            with urllib.request.urlopen(req, timeout=10) as resp:
                body = resp.read()
                status = resp.status
        except urllib.error.HTTPError as exc:
            # 上游的业务错误(如 409 粘性请求被停用拦截)必须原样透传,
            # 不能包装成 502, 否则调用方读不到真实状态码与错误原因。
            body = exc.read()
            self._send(exc.code, body)
            return
        except Exception as exc:
            body = json.dumps({"error": f"proxy upstream error: {exc}"}).encode()
            self._send(502, body)
            return
        if action == "drop_confirm":
            # 服务已经执行成功, 发布方却收不到确认: 静默关闭连接
            self.close_connection = True
            return
        if action == "flaky_fail":
            self._send(503, json.dumps({"error": "injected flaky failure"}).encode())
            return
        self._send(status, body)

    def do_GET(self):
        self._handle("GET")

    def do_POST(self):
        self._handle("POST")


def main(argv=None) -> None:
    ap = argparse.ArgumentParser(description="故障注入代理")
    ap.add_argument("--port", type=int, required=True)
    ap.add_argument("--config", required=True)
    args = ap.parse_args(argv)
    cfg = ProxyConfig(args.config)
    server = ThreadingHTTPServer(("127.0.0.1", args.port), Handler)
    Handler.cfg = cfg
    print(f"[proxy] listening on 127.0.0.1:{args.port} -> {cfg.target}", flush=True)
    server.serve_forever()


if __name__ == "__main__":
    main()

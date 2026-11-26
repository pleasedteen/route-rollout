"""本地拓扑启动器: 一次性拉起全部转发服务并写注册表。

用法:
  python -m route_release.launch up   --workdir <目录> [--proxy-node edge2 ...]
  python -m route_release.launch down --workdir <目录>

up 时在 workdir 下生成:
  registry.json   节点 -> 控制面 URL(默认直连; 指定 --proxy-node 的节点走代理)
  pids.json       进程 PID
  proxy/*.json    故障代理配置(默认 pass 模式, 测试中可在线改写)
  state/*.json    各服务持久化的生效规则
  logs/*.log      日志
"""
from __future__ import annotations

import argparse
import json
import os
import signal
import subprocess
import sys
import time
import urllib.request
from typing import Dict, List

PORT_BASE = 18000
PROXY_PORT_BASE = 19000


def _write(path: str, obj) -> None:
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(obj, fh, ensure_ascii=False, indent=2)


def _wait_healthy(url: str, timeout: float = 10.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(url + "/status", timeout=1) as resp:
                if resp.status == 200:
                    return True
        except Exception:
            time.sleep(0.1)
    return False


def cmd_up(args) -> None:
    wd = os.path.abspath(args.workdir)
    for sub in ("state", "logs", "proxy"):
        os.makedirs(os.path.join(wd, sub), exist_ok=True)
    with open(args.topology, encoding="utf-8") as fh:
        topo = json.load(fh)
    nodes: List[str] = topo["nodes"]
    proxy_nodes = set(args.proxy_node or [])
    src_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    project_dir = os.path.dirname(src_dir)
    runner = os.path.join(project_dir, "bin", "runmod.py")
    python = sys.executable

    registry: Dict[str, str] = {}
    pids: Dict[str, int] = {}
    proxy_pids: Dict[str, int] = {}

    for i, node in enumerate(nodes):
        port = PORT_BASE + i
        proxy_port = PROXY_PORT_BASE + i
        if node in proxy_nodes:
            cfg_path = os.path.join(wd, "proxy", f"{node}.json")
            _write(cfg_path, {
                "target": f"http://127.0.0.1:{port}",
                "mode": "pass", "fail_times": 1, "delay_sec": 0, "seed": i,
            })
            log = open(os.path.join(wd, "logs", f"proxy_{node}.log"),
                       "ab", buffering=0)
            proc = subprocess.Popen(
                [python, "-I", runner, "route_release.proxy",
                 "--port", str(proxy_port), "--config", cfg_path],
                cwd=project_dir, stdout=log, stderr=subprocess.STDOUT)
            proxy_pids[node] = proc.pid
            registry[node] = f"http://127.0.0.1:{proxy_port}"
        else:
            registry[node] = f"http://127.0.0.1:{port}"

    registry_path = os.path.join(wd, "registry.json")
    _write(registry_path, registry)

    for i, node in enumerate(nodes):
        port = PORT_BASE + i
        log = open(os.path.join(wd, "logs", f"service_{node}.log"),
                   "ab", buffering=0)
        cmd = [python, "-I", runner, "route_release.service",
               "--node", node, "--port", str(port),
               "--topology", os.path.abspath(args.topology),
               "--registry", registry_path,
               "--state-dir", os.path.join(wd, "state")]
        if args.initial_rules:
            cmd += ["--initial-rules", os.path.abspath(args.initial_rules)]
        proc = subprocess.Popen(cmd, cwd=project_dir, stdout=log,
                                stderr=subprocess.STDOUT)
        pids[node] = proc.pid

    ok = True
    for node, url in registry.items():
        if not _wait_healthy(url):
            print(f"服务 {node} 未在超时内就绪, 见 {wd}/logs/", file=sys.stderr)
            ok = False
    _write(os.path.join(wd, "pids.json"),
           {"services": pids, "proxies": proxy_pids})
    if not ok:
        raise SystemExit(1)
    print(f"已启动 {len(nodes)} 台转发服务, 注册表: {registry_path}")
    for node, url in sorted(registry.items()):
        via = " (经故障代理)" if node in proxy_nodes else ""
        print(f"  {node:<8} {url}{via}")


def cmd_down(args) -> None:
    wd = os.path.abspath(args.workdir)
    pids_path = os.path.join(wd, "pids.json")
    if not os.path.exists(pids_path):
        print("没有 pids.json, 无需清理")
        return
    with open(pids_path, encoding="utf-8") as fh:
        pids = json.load(fh)
    for group in ("proxies", "services"):
        for node, pid in pids.get(group, {}).items():
            try:
                os.kill(pid, signal.SIGTERM)
            except ProcessLookupError:
                pass
    time.sleep(0.5)
    for group in ("proxies", "services"):
        for node, pid in pids.get(group, {}).items():
            try:
                os.kill(pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
    os.remove(pids_path)
    print("已停止全部服务与代理")


def main(argv=None) -> None:
    ap = argparse.ArgumentParser(description="本地拓扑启动器")
    sub = ap.add_subparsers(dest="action", required=True)
    p = sub.add_parser("up")
    p.add_argument("--workdir", required=True)
    p.add_argument("--topology", required=True)
    p.add_argument("--initial-rules", default=None)
    p.add_argument("--proxy-node", action="append",
                   help="为指定节点挂故障代理, 可重复")
    p.set_defaults(func=cmd_up)
    p = sub.add_parser("down")
    p.add_argument("--workdir", required=True)
    p.set_defaults(func=cmd_down)
    args = ap.parse_args(argv)
    args.func(args)


if __name__ == "__main__":
    main()

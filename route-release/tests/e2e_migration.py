#!/usr/bin/env python3
"""固定出口迁移的端到端演练(真实本地进程)。

一段 80 秒内可跑完的命令, 输出四部分结果:
  1. 目标规则校验: 校验不过保持现网; 校验通过先出完整计划
  2. 部分切换: 单批规模/总批数上限内分批, 上一批全部生效才开下一批
  3. 跨切换请求: 一条老请求在迁移窗口内停在中继(慢夹具), 切换后仍按
     入站选定的 old 规则逐跳走到旧出口; 随后 kill -9 发布进程, 新进程
     接手排水与旧出口停用; 新请求走 new 规则到新出口
  4. 回退: 先恢复旧出口接收, 再按单批上限切回, 粘性双路探测全部正确

仅依赖 Python 3.11 标准库, 以 `python3 -I` 运行。
"""
from __future__ import annotations

import json
import os
import shutil
import signal
import subprocess
import sys
import time
import urllib.error
import urllib.request

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RUNNER = os.path.join(ROOT, "bin", "runmod.py")
EX = os.path.abspath(os.path.join(ROOT, "..", "examples"))
WD = os.path.join(ROOT, ".migration-workdir")
STATE = os.path.join(WD, "release.json")
TOPO = f"{EX}/topology.json"
OLD = f"{EX}/migration_old_rules.json"
NEW = f"{EX}/migration_new_rules.json"
BAD = f"{EX}/migration_bad_target.json"
CLS, OLD_EG, NEW_EG = "video", "hub", "relay1"

PASS, FAIL = 0, 0


def check(name: str, cond: bool, detail: str = "") -> None:
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"  PASS  {name}")
    else:
        FAIL += 1
        print(f"  FAIL  {name}  {detail}")


def run(*args, check_rc=True, timeout=60):
    proc = subprocess.run(
        [sys.executable, "-I", RUNNER, "route_release.publisher", *args],
        cwd=ROOT, capture_output=True, text=True, timeout=timeout)
    if check_rc and proc.returncode != 0:
        raise AssertionError(
            f"命令失败: {' '.join(args)}\nstdout={proc.stdout}\nstderr={proc.stderr}")
    return proc


def http(url, payload=None, retries: int = 60):
    data = json.dumps(payload).encode() if payload is not None else None
    req = urllib.request.Request(
        url, data=data,
        headers={"Content-Type": "application/json"} if data else {},
        method="POST" if data is not None else "GET")
    last = None
    for _ in range(retries):
        try:
            with urllib.request.urlopen(req, timeout=8) as resp:
                return json.loads(resp.read().decode())
        except urllib.error.HTTPError as exc:
            last = exc
            if exc.code < 500:
                # 4xx 里也可能带可读 JSON(如 409 停用拦截), 尝试返回 body
                try:
                    return json.loads(exc.read().decode())
                except Exception:
                    raise
        except Exception as exc:
            last = exc
        time.sleep(0.15)
    raise AssertionError(f"请求 {url} 重试 {retries} 次仍失败: {last}")


def registry():
    with open(os.path.join(WD, "registry.json"), encoding="utf-8") as fh:
        return json.load(fh)


def status_of(node: str) -> dict:
    return http(registry()[node] + "/status")


def versions() -> dict:
    return {n: http(u + "/status")["version"] for n, u in registry().items()}


def set_proxy_mode(node: str, mode: str, fail_times: int = 1) -> None:
    path = os.path.join(WD, "proxy", f"{node}.json")
    with open(path, encoding="utf-8") as fh:
        cfg = json.load(fh)
    cfg.update(mode=mode, fail_times=fail_times)
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(cfg, fh)


def set_slow_fixture(node: str, delay_sec: float, remaining: int = 1) -> None:
    http(registry()[node] + "/fixture",
         {"slow_classes": [CLS], "delay_sec": delay_sec, "remaining": remaining})


def probe(entry: str, pin: str = None):
    payload = {"class": CLS, "path": [], "visited": []}
    if pin:
        payload["pin"] = pin
    return http(registry()[entry] + "/probe", payload)


def wait_phase(phase: str, timeout: float = 25.0) -> dict:
    deadline = time.monotonic() + timeout
    data = {}
    while time.monotonic() < deadline:
        data = json.load(open(STATE, encoding="utf-8"))
        if data["phase"] == phase:
            return data
        time.sleep(0.15)
    raise AssertionError(f"等待阶段 {phase} 超时, 当前 {data.get('phase')}")


def main() -> int:
    if os.path.exists(WD):
        shutil.rmtree(WD)
    os.makedirs(WD)

    # ================================================================ 第一部分
    print("=" * 72)
    print("第一部分: 切入口前的完整计划与目标规则校验")
    print("=" * 72)
    p = run("submit-migration", "--topology", TOPO, "--old-rules", OLD,
            "--new-rules", BAD, "--registry", os.path.join(WD, "registry.json"),
            "--state", STATE, "--traffic-class", CLS,
            "--old-egress", OLD_EG, "--new-egress", NEW_EG,
            "--max-batch-size", "2", "--max-batches", "4", check_rc=False)
    check("目标规则不过时退出码为 2", p.returncode == 2, p.stdout)
    check("明确指出新出口未成为 local_egress",
          "local_egress" in p.stdout and NEW_EG in p.stdout, p.stdout)
    check("校验不过不创建发布状态", not os.path.exists(STATE))

    subprocess.run([sys.executable, "-I", RUNNER, "route_release.launcher", "up",
                    "--workdir", WD, "--topology", TOPO,
                    "--initial-rules", OLD,
                    "--proxy-node", "edge2", "--proxy-node", "edge4"],
                   cwd=ROOT, check=True)
    try:
        reg = registry()
        before = versions()
        p = run("submit-migration", "--topology", TOPO, "--old-rules", OLD,
                "--new-rules", NEW, "--registry", os.path.join(WD, "registry.json"),
                "--state", STATE, "--traffic-class", CLS,
                "--old-egress", OLD_EG, "--new-egress", NEW_EG,
                "--max-batch-size", "2", "--max-batches", "4")
        plan = json.load(open(STATE, encoding="utf-8"))["forward_plan"]
        check("提交后打印完整批次计划", "批次" in p.stdout and "目标闸" in p.stdout)
        check("批数不超过上限 4", len(plan["batches"]) <= 4)
        check("单批规模不超过上限 2", all(len(b) <= 2 for b in plan["batches"]))
        check("计划覆盖全部 7 台服务",
              sorted(sum(plan["batches"], [])) == sorted(reg))
        check("提交计划不触碰现网配置", versions() == before)
        mig = json.load(open(STATE, encoding="utf-8"))["migration"]
        check("状态记录了迁移类别与新旧出口",
              mig == {"class": CLS, "old_egress": OLD_EG, "new_egress": NEW_EG})

        # ================================================================ 第二部分
        print("=" * 72)
        print("第二部分: 部分切换(批次中途 kill -9, 上一批生效后才开下一批)")
        print("=" * 72)
        # edge4 抖动制造可观察的部分生效窗口
        set_proxy_mode("edge4", "flaky", fail_times=50)
        proc = subprocess.Popen(
            [sys.executable, "-I", RUNNER, "route_release.publisher", "run",
             "--state", STATE], cwd=ROOT,
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        deadline = time.monotonic() + 15
        new_doc = json.load(open(NEW, encoding="utf-8"))
        switched = []
        while time.monotonic() < deadline:
            time.sleep(0.1)
            cur = versions()
            switched = [n for n in reg
                        if cur[n] != before[n]]
            if 1 <= len(switched) < len(reg):
                break
        proc.send_signal(signal.SIGKILL)
        proc.wait()
        set_proxy_mode("edge4", "pass")
        mid = versions()
        switched = sorted(n for n in reg if mid[n] != before[n])
        check("崩溃时仅一部分服务切换(部分生效)",
              1 <= len(switched) < len(reg), str(switched))
        data = json.load(open(STATE, encoding="utf-8"))
        first_batch = set(plan["batches"][0])
        # 已切换节点只能来自已开启的批次(上一批未整体生效不开新批)
        opened: set = set()
        for batch in plan["batches"]:
            opened |= set(batch)
            if not (opened <= set(switched)) and not (set(switched) <= opened):
                break
        check("部分切换只发生在已开启批次内", set(switched) <= opened,
              f"{switched} vs {sorted(opened)}")
        # 混合状态下逐跳探测仍全部成功(任意部分生效都可达)
        ok_mixed = True
        for entry in reg:
            res = probe(entry)
            ok_mixed = ok_mixed and res.get("ok") is True
        check("混合状态下无 pin 探测仍全部可达", ok_mixed)

        # ================================================================ 第三部分
        print("=" * 72)
        print("第三部分: 跨切换粘性请求 + 新进程接手排水与旧出口停用")
        print("=" * 72)
        # 老请求在 relay1 上慢 4 秒: 发起后先停在中继, 期间让发布继续切换
        set_slow_fixture("relay1", delay_sec=4.0, remaining=1)
        old_request = None
        import threading

        def fire_old_request():
            nonlocal old_request
            old_request = probe("edge1", pin="old")

        t = threading.Thread(target=fire_old_request)
        t.start()
        time.sleep(1.0)  # 确保老请求已经进入 relay1 并在夹具上等待
        set_proxy_mode("edge2", "drop_confirm")  # 接手时顺带验证确认丢失对账
        run("resume", "--state", STATE, timeout=90)
        t.join(timeout=15)
        set_proxy_mode("edge2", "pass")  # 后续探测不再注入故障
        check("跨切换老请求按 old 规则逐跳走到旧出口 hub",
              old_request is not None and old_request.get("ok")
              and old_request.get("egress") == OLD_EG
              and old_request.get("pin") == "old",
              str(old_request))
        check("老请求路径每一跳都记录在案",
              old_request and old_request["path"][0] == "edge1"
              and old_request["path"][-1] == OLD_EG,
              str(old_request and old_request.get("path")))

        final = versions()
        new_tokens = {n: http(reg[n] + "/status")["version_by_label"]["new"]
                      for n in reg}
        check("接手后全部服务到达 new 版本",
              all(final[n] == new_tokens[n] for n in reg), str(final))
        data = wait_phase("DECOMMISSIONED", timeout=20)
        check("发布进程接手后完成排水并停用旧出口",
              data["phase"] == "DECOMMISSIONED")
        hub_status = status_of("hub")
        check("hub 已停用 video 类别的 old 接收",
              {"class": CLS, "label": "old"} in hub_status["decommissioned"])
        check("停用后无 old 在途请求",
              hub_status["inflight"].get("old", 0) == 0
              and hub_status["visiting"].get("old", 0) == 0,
              str(hub_status))
        # 新请求按 new 规则走到新出口 relay1
        new_req = probe("edge4")
        check("新入站请求按 new 规则走到新出口 relay1",
              new_req.get("ok") and new_req.get("egress") == NEW_EG
              and new_req.get("pin") == "new", str(new_req))
        # 旧出口接收已停用: 显式 old 粘性请求被明确拒绝(在旧出口或其前置跳
        # 收到 409), 而不是被错误转发到新出口
        rejected = probe("edge2", pin="old")
        check("停用后 old 粘性请求被明确拦截(409, 未走到新出口)",
              rejected.get("ok") is False
              and (rejected.get("egress") != NEW_EG)
              and ("停用" in rejected.get("error", "")
                   or "409" in rejected.get("error", "")),
              str(rejected))
        # 非迁移类别 voice/data 仍正常到达静态出口 hub
        voice = http(reg["edge1"] + "/probe",
                     {"class": "voice", "path": [], "visited": []})
        check("非迁移类别 voice 仍到达静态出口 hub",
              voice.get("ok") and voice.get("egress") == "hub", str(voice))

        # 重启一台服务: 保留规则集与停用项持久化, 接手信息不丢
        pids = json.load(open(os.path.join(WD, "pids.json"), encoding="utf-8"))
        relay2_pid = pids["services"]["relay2"]
        os.kill(relay2_pid, signal.SIGTERM)
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            try:
                http(reg["relay2"] + "/status")
                time.sleep(0.1)
            except Exception:
                break
        port = 18000 + list(reg).index("relay2")
        log = open(os.path.join(WD, "logs", "service_relay2.log"), "ab", buffering=0)
        new_proc = subprocess.Popen(
            [sys.executable, "-I", RUNNER, "route_release.service",
             "--node", "relay2", "--port", str(port),
             "--topology", TOPO,
             "--registry", os.path.join(WD, "registry.json"),
             "--state-dir", os.path.join(WD, "state")],
            cwd=ROOT, stdout=log, stderr=subprocess.STDOUT)
        pids["services"]["relay2"] = new_proc.pid
        json.dump(pids, open(os.path.join(WD, "pids.json"), "w", encoding="utf-8"))
        deadline = time.monotonic() + 8
        restored = None
        while time.monotonic() < deadline:
            try:
                restored = status_of("relay2")
                break
            except Exception:
                time.sleep(0.15)
        check("服务重启后保留 old/new 两套规则集",
              restored and set(restored.get("labels", [])) >= {"old", "new"},
              str(restored and restored.get("labels")))
        check("服务重启后当前标签仍为 new",
              restored and restored.get("current_label") == "new")

        # status/verify 可查询实际生效配置与粘性双路
        st = run("status", "--state", STATE)
        check("status 展示迁移信息与在途/停用状态",
              "固定出口迁移" in st.stdout and "DECOMMISSIONED" in st.stdout)
        vp = run("verify", "--state", STATE)
        # 7 入口 x 迁移类别双 pin = 14 条(其中 old 路 7 条为预期拦截),
        # 另两类 x 7 = 14 条正常探测
        ok_lines = vp.stdout.count(" OK")
        check("verify 覆盖粘性双路与非迁移类别(28 条全 OK)",
              ok_lines == 28, f"{ok_lines} 条 OK\n{vp.stdout[-500:]}")

        # ================================================================ 第四部分
        print("=" * 72)
        print("第四部分: 按单批上限回退(先恢复旧出口接收, 再逐批切回)")
        print("=" * 72)
        set_proxy_mode("edge4", "drop_request", fail_times=1)
        run("rollback", "--state", STATE, timeout=90)
        rolled = versions()
        check("回退后全部恢复发布前版本",
              all(rolled[n] == before[n] for n in reg), str(rolled))
        rb_plan = json.load(open(STATE, encoding="utf-8"))["rollback_plan"]
        check("回退批数与单批规模仍受两项上限约束",
              len(rb_plan["batches"]) <= 4
              and all(len(b) <= 2 for b in rb_plan["batches"]))
        check("旧出口 hub 在回退第一批(先恢复接收)",
              OLD_EG in rb_plan["batches"][0], str(rb_plan["batches"]))
        check("入口节点不进回退第一批",
              all(n not in rb_plan["batches"][0]
                  for n in reg if n not in (OLD_EG, NEW_EG)))
        hub_status = status_of("hub")
        check("回退后 hub 的 video old 停用项已清除",
              {"class": CLS, "label": "old"} not in hub_status["decommissioned"])
        old_again = probe("edge1", pin="old")
        check("回退后 old 粘性请求重新走到旧出口 hub",
              old_again.get("ok") and old_again.get("egress") == OLD_EG,
              str(old_again))
        data = json.load(open(STATE, encoding="utf-8"))
        check("发布阶段为 ROLLED_BACK", data["phase"] == "ROLLED_BACK")
        run("verify", "--state", STATE)
        check("回退后实时探测全部可达", True)

    finally:
        subprocess.run([sys.executable, "-I", RUNNER, "route_release.launcher",
                        "down", "--workdir", WD], cwd=ROOT)

    print("=" * 72)
    print(f"结果: {PASS} PASS / {FAIL} FAIL")
    print("=" * 72)
    if FAIL == 0:
        shutil.rmtree(WD, ignore_errors=True)
    else:
        print(f"有失败项, 保留现场: {WD}")
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(main())

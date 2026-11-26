#!/usr/bin/env python3
"""端到端测试: 在真实本地进程上复现部分生效/确认丢失并验证恢复能力。

由 run_demo.sh 以 `python3 -I e2e_demo.py` 方式运行, 全程约 15-30 秒。
不依赖第三方库, 结果以可读的 PASS/FAIL 输出。
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
SRC = os.path.join(ROOT, "src")
RUNNER = os.path.join(ROOT, "bin", "runmod.py")
EX = os.path.join(ROOT, "..", "examples")
EX = os.path.abspath(EX)
WD = os.path.join(ROOT, ".demo-workdir")
STATE = os.path.join(WD, "release.json")

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
    """测试侧 HTTP 辅助: 对 flaky 代理注入的瞬时 5xx/连接错误做有限重试。

    发布工具自身的重试/对账是另一套逻辑(publisher.http_json + run 循环),
    这里只是让测试查询能穿透故障窗口读到服务的稳定状态。
    """
    data = json.dumps(payload).encode() if payload is not None else None
    req = urllib.request.Request(
        url, data=data,
        headers={"Content-Type": "application/json"} if data else {},
        method="POST" if data is not None else "GET")
    last = None
    for _ in range(retries):
        try:
            with urllib.request.urlopen(req, timeout=5) as resp:
                return json.loads(resp.read().decode())
        except urllib.error.HTTPError as exc:
            last = exc
            if exc.code < 500:
                raise
        except Exception as exc:  # 连接被重置/拒绝等瞬时故障
            last = exc
        time.sleep(0.15)
    raise AssertionError(f"请求 {url} 重试 {retries} 次仍失败: {last}")


def set_proxy_mode(node: str, mode: str, fail_times: int = 1) -> None:
    path = os.path.join(WD, "proxy", f"{node}.json")
    with open(path, encoding="utf-8") as fh:
        cfg = json.load(fh)
    cfg.update(mode=mode, fail_times=fail_times)
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(cfg, fh)
    # 代理每次请求都重新读配置, 无需重启


def compute_tokens(rules_doc: dict) -> dict:
    """在子进程里用项目代码计算规则指纹(-I 下测试脚本本身不能 import 包)。"""
    code = (
        "import json, sys; sys.path.insert(0, sys.argv[1]);"
        "from route_release.model import ruleset_from_plain, version_token;"
        "doc = json.load(open(sys.argv[2], encoding='utf-8'));"
        "print(json.dumps({n: version_token(ruleset_from_plain(r)) "
        "for n, r in doc.items()}))"
    )
    tmp = os.path.join(WD, "_rules_doc.json")
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(rules_doc, fh)
    out = subprocess.run([sys.executable, "-I", "-c", code, SRC, tmp],
                         capture_output=True, text=True, check=True)
    return json.loads(out.stdout)


def status_versions():
    reg = json.load(open(os.path.join(WD, "registry.json"), encoding="utf-8"))
    return {n: http(u + "/status")["version"] for n, u in reg.items()}


def wait_phase(phase: str, timeout: float = 20.0) -> dict:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        data = json.load(open(STATE, encoding="utf-8"))
        if data["phase"] == phase:
            return data
        time.sleep(0.15)
    raise AssertionError(f"等待阶段 {phase} 超时, 当前 {data['phase']}")


def main() -> int:
    if os.path.exists(WD):
        shutil.rmtree(WD)
    os.makedirs(WD)

    print("=" * 72)
    print("场景 0: 无解规划(3 台一批 / 总批 1, 但同批部分切换会成环)")
    print("=" * 72)
    p = run("plan", "--topology", f"{EX}/infeasible_topology.json",
            "--old-rules", f"{EX}/infeasible_old.json",
            "--new-rules", f"{EX}/infeasible_new.json",
            "--max-batch-size", "3", "--max-batches", "1", check_rc=False)
    check("无解时返回非零退出码", p.returncode == 2, p.stdout)
    check("给出可读原因(环路 + 最少批数)",
          "环" in p.stdout and "至少需要" in p.stdout, p.stdout)
    p2 = run("plan", "--topology", f"{EX}/infeasible_topology.json",
             "--old-rules", f"{EX}/infeasible_old.json",
             "--new-rules", f"{EX}/infeasible_new.json",
             "--max-batch-size", "3", "--max-batches", "3", check_rc=False)
    check("放宽批数上限后有解(建议顺序中 a 先于 b)",
          p2.returncode == 0 and "a" in p2.stdout, p2.stdout)

    print("=" * 72)
    print("场景 1: 拉起 7 台服务, edge2/edge4 挂故障代理")
    print("=" * 72)
    subprocess.run([sys.executable, "-I", RUNNER, "route_release.launcher", "up",
                    "--workdir", WD, "--topology", f"{EX}/topology.json",
                    "--initial-rules", f"{EX}/old_rules.json",
                    "--proxy-node", "edge2", "--proxy-node", "edge4"],
                   cwd=ROOT, check=True)
    try:
        reg = json.load(open(os.path.join(WD, "registry.json"), encoding="utf-8"))
        old = json.load(open(f"{EX}/old_rules.json", encoding="utf-8"))
        new = json.load(open(f"{EX}/new_rules.json", encoding="utf-8"))
        old_tok = compute_tokens(old)
        new_tok = compute_tokens(new)
        versions = status_versions()
        check("全部服务以发布前配置启动",
              all(versions[n] == old_tok[n] for n in reg), str(versions))

        print("=" * 72)
        print("场景 2: 提交计划(单批 2 台 / 最多 4 批), 不动服务先出完整计划")
        print("=" * 72)
        p = run("submit", "--topology", f"{EX}/topology.json",
                "--old-rules", f"{EX}/old_rules.json",
                "--new-rules", f"{EX}/new_rules.json",
                "--registry", os.path.join(WD, "registry.json"),
                "--state", STATE, "--max-batch-size", "2", "--max-batches", "4")
        plan = json.load(open(STATE, encoding="utf-8"))["forward_plan"]
        check("批数不超过上限", len(plan["batches"]) <= 4)
        check("单批规模不超过上限", all(len(b) <= 2 for b in plan["batches"]))
        check("计划覆盖全部 7 台",
              sorted(sum(plan["batches"], [])) == sorted(reg))
        check("提交后服务配置未被触碰", status_versions() == versions)

        print("=" * 72)
        print("场景 3: 部分生效——发布进程在批次中途被 kill -9")
        print("=" * 72)
        # edge4 注入故障使发布推进变慢, 留出"部分生效"的可观察窗口
        set_proxy_mode("edge4", "flaky", fail_times=50)
        proc = subprocess.Popen(
            [sys.executable, "-I", RUNNER, "route_release.publisher", "run",
             "--state", STATE], cwd=ROOT,
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            time.sleep(0.1)
            switched_now = [n for n, v in status_versions().items()
                            if v == new_tok[n]]
            if 1 <= len(switched_now) < len(reg):
                break
        proc.send_signal(signal.SIGKILL)  # 不给优雅退出机会, 模拟进程崩溃
        proc.wait()
        set_proxy_mode("edge4", "pass")
        mid = status_versions()
        switched = [n for n in reg if mid[n] == new_tok[n]]
        check("崩溃时只有一部分服务切换(部分生效)",
              1 <= len(switched) < len(reg), str(switched))

        print("=" * 72)
        print("场景 4: 新进程接手, 先对账再续跑(确认丢失场景随后注入)")
        print("=" * 72)
        set_proxy_mode("edge2", "drop_confirm")  # edge2 的 apply 将成功但无响应
        run("resume", "--state", STATE)
        final = status_versions()
        check("接手后全部服务到达目标版本",
              all(final[n] == new_tok[n] for n in reg), str(final))
        data = json.load(open(STATE, encoding="utf-8"))
        check("发布阶段为 FORWARD_COMPLETE", data["phase"] == "FORWARD_COMPLETE")
        log_out = subprocess.run(
            [sys.executable, "-I", RUNNER, "route_release.publisher", "status",
             "--state", STATE], cwd=ROOT, capture_output=True, text=True).stdout
        check("status 可查每台实际版本与进度", "新配置已生效" in log_out)

        print("=" * 72)
        print("场景 5: 实时逐跳探测, 各类流量到达固定出口")
        print("=" * 72)
        vp = run("verify", "--state", STATE)
        check("21 条探测全部 OK(7 节点 x 3 类别)",
              vp.stdout.count(" OK") == 21, vp.stdout[-400:])

        print("=" * 72)
        print("场景 6: 暂停/继续语义")
        print("=" * 72)
        # 制造新的回退进度来演示暂停: 先发起回退再暂停
        set_proxy_mode("edge2", "pass")
        set_proxy_mode("edge4", "flaky", fail_times=2)
        rb = subprocess.Popen(
            [sys.executable, "-I", RUNNER, "route_release.publisher", "rollback",
             "--state", STATE], cwd=ROOT,
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        time.sleep(0.8)
        pp = run("pause", "--state", STATE)
        rb.wait(timeout=30)
        paused = status_versions()
        data = json.load(open(STATE, encoding="utf-8"))
        check("暂停后阶段为 PAUSED 或已到 ROLLED_BACK",
              data["phase"] in ("PAUSED", "ROLLED_BACK"), data["phase"])
        if data["phase"] == "PAUSED":
            check("暂停后没有出现整批之外的混乱版本(仅新/老两种指纹)",
                  set(paused.values()) <= set(old_tok.values()) | set(new_tok.values()))
            run("resume", "--state", STATE)
        after = status_versions()
        check("继续后回退完成, 全部恢复发布前配置",
              all(after[n] == old_tok[n] for n in reg), str(after))
        check("阶段为 ROLLED_BACK",
              json.load(open(STATE, encoding="utf-8"))["phase"] == "ROLLED_BACK")

        print("=" * 72)
        print("场景 7: 服务重启后生效配置保留")
        print("=" * 72)
        before = status_versions()
        # 重启 edge3: 杀进程再按同样端口拉起(状态文件存在, 不带 initial-rules)
        pids = json.load(open(os.path.join(WD, "pids.json"), encoding="utf-8"))
        edge3_pid = pids["services"]["edge3"]
        try:
            os.kill(edge3_pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
        # 以端口真正释放为准, 避免重启竞态
        kill_deadline = time.monotonic() + 5
        while time.monotonic() < kill_deadline:
            try:
                http(reg["edge3"] + "/status")
                time.sleep(0.1)
            except Exception:
                break
        port = 18000 + list(reg).index("edge3")
        log = open(os.path.join(WD, "logs", "service_edge3.log"), "ab", buffering=0)
        new_proc = subprocess.Popen(
            [sys.executable, "-I", RUNNER, "route_release.service",
             "--node", "edge3", "--port", str(port),
             "--topology", f"{EX}/topology.json",
             "--registry", os.path.join(WD, "registry.json"),
             "--state-dir", os.path.join(WD, "state")],
            cwd=ROOT, stdout=log, stderr=subprocess.STDOUT)
        pids["services"]["edge3"] = new_proc.pid
        with open(os.path.join(WD, "pids.json"), "w", encoding="utf-8") as fh:
            json.dump(pids, fh)
        deadline = time.monotonic() + 8
        restored = None
        while time.monotonic() < deadline:
            try:
                restored = http(reg["edge3"] + "/status")["version"]
                break
            except Exception:
                time.sleep(0.15)
        check("edge3 重启后自动恢复生效配置", restored == before["edge3"],
              f"{restored} != {before['edge3']}")

        print("=" * 72)
        print("场景 8: 再次正向发布 + 完成后回退, 回退也受批次/可达性约束")
        print("=" * 72)
        os.remove(STATE)
        run("submit", "--topology", f"{EX}/topology.json",
            "--old-rules", f"{EX}/old_rules.json",
            "--new-rules", f"{EX}/new_rules.json",
            "--registry", os.path.join(WD, "registry.json"),
            "--state", STATE, "--max-batch-size", "2", "--max-batches", "4")
        run("run", "--state", STATE)
        check("正向发布完成",
              wait_phase("FORWARD_COMPLETE")["phase"] == "FORWARD_COMPLETE")
        set_proxy_mode("edge4", "drop_request", fail_times=1)
        run("rollback", "--state", STATE)
        rolled = status_versions()
        check("回退后全部为发布前版本",
              all(rolled[n] == old_tok[n] for n in reg), str(rolled))
        rb_plan = json.load(open(STATE, encoding="utf-8"))["rollback_plan"]
        check("回退计划同样满足两项上限",
              len(rb_plan["batches"]) <= 4
              and all(len(b) <= 2 for b in rb_plan["batches"]))
        run("verify", "--state", STATE)
        check("回退后实时探测仍全部可达", True)

    finally:
        subprocess.run([sys.executable, "-I", RUNNER, "route_release.launcher",
                        "down", "--workdir", WD], cwd=ROOT)

    print("=" * 72)
    print(f"结果: {PASS} PASS / {FAIL} FAIL")
    print("=" * 72)
    if FAIL == 0:
        shutil.rmtree(WD, ignore_errors=True)  # 成功后清理; 失败则保留现场便于排查
    else:
        print(f"有失败项, 保留现场: {WD}")
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(main())

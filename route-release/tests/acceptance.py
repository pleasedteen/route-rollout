#!/usr/bin/env python3
"""本地验收: 用故障夹具复现"正向切换卡在代理里"的现场, 逐条验收安全语义。

覆盖(全部在真实本地进程上跑):
  A. 延迟投递 + 暂停: 正向 apply 还被代理扣着时暂停, 暂停不得提前报完成;
     暂停返回后迟到的旧请求必须被栅栏永久拒止, 不能改写结果。
  B. 回退: 只在暂停/发布完成后按各节点**实际**规则规划; 满足单批规模上限,
     迟到/重发的正向请求不能覆盖回退结果。
  C. 丢确认 + 进程接手: 发布进程在 apply 在途时被 kill -9, 新进程接手未决
     操作, 按服务实际生效结果收尾。
  D. 超时未决可查: 节点失联导致暂停等待超时后, 未决状态仍可由 status 查到。

由 acceptance.sh 以 `python3 -I tests/acceptance.py` 运行, 要求 80 秒内完成。
不依赖第三方库, 输出可读的 PASS/FAIL。
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
EX = os.path.abspath(os.path.join(ROOT, "..", "examples"))
WD = os.path.join(ROOT, ".acceptance-workdir")
STATE = os.path.join(WD, "release.json")
PROXY_NODES = ("edge2", "edge4")

PASS, FAIL = 0, 0
START = time.monotonic()


def check(name: str, cond: bool, detail: str = "") -> None:
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"  PASS  {name}")
    else:
        FAIL += 1
        print(f"  FAIL  {name}  {detail}")


def section(title: str) -> None:
    print("=" * 72)
    print(title)
    print("=" * 72)


def run(*args, check_rc=True, timeout=60):
    proc = subprocess.run(
        [sys.executable, "-I", RUNNER, "route_release.publisher", *args],
        cwd=ROOT, capture_output=True, text=True, timeout=timeout)
    if check_rc and proc.returncode != 0:
        raise AssertionError(
            f"命令失败(rc={proc.returncode}): {' '.join(args)}\n"
            f"stdout={proc.stdout}\nstderr={proc.stderr}")
    return proc


def spawn(*args):
    return subprocess.Popen(
        [sys.executable, "-I", RUNNER, "route_release.publisher", *args],
        cwd=ROOT, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)


def http(url, payload=None, retries: int = 40, timeout: float = 5):
    data = json.dumps(payload).encode() if payload is not None else None
    req = urllib.request.Request(
        url, data=data,
        headers={"Content-Type": "application/json"} if data else {},
        method="POST" if data is not None else "GET")
    last = None
    for _ in range(retries):
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return resp.status, json.loads(resp.read().decode())
        except urllib.error.HTTPError as exc:
            # 4xx 是业务语义(如栅栏拒止的 409), 直接返回给调用方判断
            if exc.code < 500:
                return exc.code, json.loads(exc.read().decode())
            last = exc
        except Exception as exc:  # 连接重置/拒绝等瞬时故障
            last = exc
        time.sleep(0.1)
    raise AssertionError(f"请求 {url} 重试 {retries} 次仍失败: {last}")


def registry() -> dict:
    with open(os.path.join(WD, "registry.json"), encoding="utf-8") as fh:
        return json.load(fh)


def status_versions() -> dict:
    return {n: http(u + "/status")[1]["version"] for n, u in registry().items()}


def proxy_set(node: str, **fields) -> None:
    path = os.path.join(WD, "proxy", f"{node}.json")
    with open(path, encoding="utf-8") as fh:
        cfg = json.load(fh)
    cfg.update(fields)
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(cfg, fh)
    # mtime 可能同秒不变, 轻推一下确保代理热加载
    os.utime(path, None)


def state_read() -> dict:
    with open(STATE, encoding="utf-8") as fh:
        return json.load(fh)


def pending() -> dict:
    return state_read().get("pending", {})


def wait_for(pred, timeout: float, desc: str):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        val = pred()
        if val:
            return val
        time.sleep(0.05)
    raise AssertionError(f"等待 {desc} 超时")


def compute_tokens(rules_doc: dict) -> dict:
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


def store_probe(state_path: str, op: str) -> dict:
    """在隔离子进程里直接验证状态存储的跨进程语义(模拟"接手的 CLI")。"""
    code = (
        "import json, sys; sys.path.insert(0, sys.argv[1]);"
        "from route_release.state import ReleaseStore;"
        "s = ReleaseStore(sys.argv[2]); op = sys.argv[3];"
        "out = {};"
        "\nif op == 'begin':\n"
        "    out['epoch'] = s.begin_op('edge1', direction='forward', batch=[],"
        " target='x', started_at='probe')"
        "\nelif op == 'pause':\n"
        "    e, r = s.accept_pause(); out = {'epoch': e, 'result': r}"
        "\nprint(json.dumps(out))"
    )
    out = subprocess.run([sys.executable, "-I", "-c", code, SRC, state_path, op],
                         capture_output=True, text=True, check=True)
    return json.loads(out.stdout)


def head(txt: str, n: int = 4) -> str:
    lines = [ln for ln in txt.splitlines() if ln.strip()]
    return " | ".join(lines[:n])


def main() -> int:
    if os.path.exists(WD):
        shutil.rmtree(WD)
    os.makedirs(WD)

    old = json.load(open(f"{EX}/old_rules.json", encoding="utf-8"))
    new = json.load(open(f"{EX}/new_rules.json", encoding="utf-8"))
    old_tok = compute_tokens(old)
    new_tok = compute_tokens(new)

    subprocess.run([sys.executable, "-I", RUNNER, "route_release.launcher", "up",
                    "--workdir", WD, "--topology", f"{EX}/topology.json",
                    "--initial-rules", f"{EX}/old_rules.json",
                    "--proxy-node", "edge2", "--proxy-node", "edge4"],
                   cwd=ROOT, check=True, capture_output=True)
    reg = registry()
    p1 = None
    p2 = None
    p4 = None
    try:
        check("拓扑以发布前配置启动",
              all(v == old_tok[n] for n, v in status_versions().items()))

        # ------------------------------------------------------------ 场景 A
        section("场景 A: 延迟投递 + 暂停(卡在代理里的正向 apply)")
        run("submit", "--topology", f"{EX}/topology.json",
            "--old-rules", f"{EX}/old_rules.json",
            "--new-rules", f"{EX}/new_rules.json",
            "--registry", os.path.join(WD, "registry.json"),
            "--state", STATE, "--max-batch-size", "2", "--max-batches", "4")
        plan = state_read()["forward_plan"]
        check("正向计划满足两项上限",
              len(plan["batches"]) <= 4 and all(len(b) <= 2 for b in plan["batches"]))
        # 第一只延迟的节点必须落在首个批次里, 这样 run 一启动就会在它身上
        # 登记 pending; 规划的搜索顺序确定(edge1, edge2 同批)。
        proxy_in_first = [n for n in plan["batches"][0] if n in PROXY_NODES]
        assert proxy_in_first, f"首个批次不含故障代理节点: {plan['batches'][0]}"
        delayed_node = proxy_in_first[0]

        # 让该节点的 apply 在代理里被扣住 2.5 秒
        proxy_set(delayed_node, mode="pass", fail_times=1,
                  delay_sec=2.5, delay_times=1)
        p1 = spawn("run", "--state", STATE)
        pend = wait_for(lambda: pending() if delayed_node in pending() else None,
                        8, "在途操作登记(pending)")
        check("下发前先把操作登记为未决", delayed_node in pend, str(list(pend)))
        op_epoch = pend[delayed_node]["epoch"]

        # 此时是 RUNNING, 不允许直接回退
        early_rb = run("rollback", "--state", STATE, check_rc=False)
        early_out = early_rb.stdout + early_rb.stderr
        check("正向未完成/未暂停时拒绝回退",
              early_rb.returncode != 0 and "不能回退" in early_out,
              head(early_out))

        # 在请求仍在代理里时暂停
        t_pause = time.monotonic()
        pp = run("pause", "--state", STATE, "--timeout", "15")
        pause_secs = time.monotonic() - t_pause
        data = state_read()
        check("暂停返回成功且阶段为 PAUSED",
              pp.returncode == 0 and data["phase"] == "PAUSED",
              f"rc={pp.returncode} phase={data['phase']}")
        check("暂停完成时不再有未决操作(没有提前报完成)",
              data.get("pending", {}) == {}, str(data.get("pending")))
        check("暂停输出为'完成'而非'超时'",
              "暂停完成" in pp.stdout and "等待超时" not in pp.stdout,
              head(pp.stdout))

        # 暂停后的确定性: 给该节点装的栅栏必须压过在途请求的代数
        floor = http(reg[delayed_node] + "/status")[1]["epoch_floor"]
        check("暂停给节点装了压过在途请求的栅栏",
              floor > op_epoch, f"floor={floor} op_epoch={op_epoch}")

        versions_at_pause = status_versions()
        # 等过原定延迟, 迟到的请求应已被拒, 版本不再变化
        time.sleep(3.0)
        p1.wait(timeout=10)
        after_delay = status_versions()
        check("迟到请求未改写暂停后的版本",
              after_delay == versions_at_pause, f"{after_delay} != {versions_at_pause}")
        check("暂停后各节点仅处于明确的新/老版本",
              set(after_delay.values()) <= set(old_tok.values()) | set(new_tok.values()))
        check("暂停时确实只切了一部分(混合现场)",
              0 < sum(after_delay[n] == new_tok[n] for n in reg) < len(reg),
              str({n: after_delay[n] == new_tok[n] for n in reg}))

        # 接手的并行 CLI 无法再开新批: phase=PAUSED 时 begin_op 必被拒
        probe = store_probe(STATE, "begin")
        check("暂停已接受后接手的 CLI 无法登记新操作(不开新批)",
              probe.get("epoch") is None, str(probe))
        pause_probe = store_probe(STATE, "pause")
        check("暂停是幂等的(复位为 already)",
              pause_probe.get("result") == "already", str(pause_probe))

        # ------------------------------------------------------------ 场景 B
        section("场景 B: 按实际混合现场规划回退 + 迟到/重发请求不得覆盖")
        rb = run("rollback", "--state", STATE)
        rb_plan = state_read()["rollback_plan"]
        check("回退完成, 全部恢复发布前配置",
              all(status_versions()[n] == old_tok[n] for n in reg),
              str(status_versions()))
        check("回退计划满足两项上限",
              len(rb_plan["batches"]) <= 4
              and all(len(b) <= 2 for b in rb_plan["batches"]))
        check("回退计划以各节点实际版本为起点",
              all(rb_plan["from_tokens"][n] == versions_at_pause[n] for n in reg),
              f"{rb_plan['from_tokens']} vs {versions_at_pause}")

        # 重放一条"迟到/重发"的正向 apply(旧代数), 不得覆盖回退结果
        stale_node = next(n for n in reg if n in PROXY_NODES)
        code, body = http(reg[stale_node] + "/apply",
                          {"rules": new[stale_node], "epoch": 1})
        check("旧代数的正向请求被栅栏拒绝",
              code == 409 and body.get("stale") is True, f"{code} {body}")
        check("被拒的迟到请求没有改写回退结果",
              http(reg[stale_node] + "/status")[1]["version"] == old_tok[stale_node])

        # ------------------------------------------------------------ 场景 C
        section("场景 C: 丢确认 + 进程接手(apply 在途时 kill -9)")
        os.remove(STATE)
        run("submit", "--topology", f"{EX}/topology.json",
            "--old-rules", f"{EX}/old_rules.json",
            "--new-rules", f"{EX}/new_rules.json",
            "--registry", os.path.join(WD, "registry.json"),
            "--state", STATE, "--max-batch-size", "2", "--max-batches", "4")
        proxy_set(delayed_node, mode="pass", fail_times=1,
                  delay_sec=2.7, delay_times=1)
        p2 = spawn("run", "--state", STATE)
        pend = wait_for(lambda: (pending() or None), 8, "接手前的在途操作")
        victim = next(iter(pend))
        victim_epoch = pend[victim]["epoch"]
        p2.send_signal(signal.SIGKILL)  # 确认来不及送达
        p2.wait()
        # 代理仍会把这条请求投递给服务: 确认丢了, 但操作实际生效
        landed = wait_for(
            lambda: http(reg[victim] + "/status")[1]["version"] == new_tok[victim],
            8, "在途请求在发布进程死后落地")
        check("发布进程死后, 在途请求仍实际生效(确认丢失)",
              landed and state_read()["pending"].get(victim) is not None,
              str(state_read()["pending"]))
        # 去掉延迟, 新进程接手
        proxy_set(delayed_node, mode="pass", fail_times=1, delay_sec=0, delay_times=0)
        resumed = run("resume", "--state", STATE, "--max-seconds", "60")
        final = status_versions()
        data = state_read()
        check("新进程接手后全部到达目标版本",
              all(final[n] == new_tok[n] for n in reg), str(final))
        check("新进程按实际生效结果收尾(未决清空, 阶段完成)",
              data["phase"] == "FORWARD_COMPLETE" and data.get("pending") == {},
              f"phase={data['phase']} pending={data.get('pending')}")
        check("接手进程没有把已生效操作当成失败重做",
              resumed.returncode == 0)

        # ------------------------------------------------------------ 场景 D
        section("场景 D: 节点失联导致暂停超时, 未决状态仍可查")
        proxy_set("edge4", mode="pass", fail_times=1,
                  delay_sec=4.0, delay_times=1)
        p4 = spawn("rollback", "--state", STATE)
        wait_for(lambda: "edge4" in pending(), 12, "edge4 的回退操作登记为未决")
        edge4_epoch = pending()["edge4"]["epoch"]
        # 让 edge4 彻底失联(连 /status、/fence 都不可达)
        proxy_set("edge4", blackhole=True)
        t_pause = time.monotonic()
        dp = run("pause", "--state", STATE, "--timeout", "1", check_rc=False)
        pause_secs = time.monotonic() - t_pause
        data = state_read()
        check("失联节点使暂停等待超时并以非零退出",
              dp.returncode != 0 and "超时" in dp.stdout,
              f"rc={dp.returncode} {head(dp.stdout)}")
        check("超时后未决状态仍保留在状态文件",
              "edge4" in data.get("pending", {}), str(data.get("pending")))
        st = run("status", "--state", STATE, check_rc=False)
        check("status 可查到未决操作(节点/代数/目标)",
              "未决操作" in st.stdout and "edge4" in st.stdout
              and str(data["pending"]["edge4"]["epoch"]) in st.stdout,
              head(st.stdout, 20))
        # 恢复节点, 清理在途请求并完成回退
        proxy_set("edge4", blackhole=False, delay_sec=0, delay_times=0)
        p4.wait(timeout=15)
        run("resume", "--state", STATE, "--max-seconds", "60")
        check("节点恢复后续跑完成回退",
              state_read()["phase"] == "ROLLED_BACK"
              and all(status_versions()[n] == old_tok[n] for n in reg),
              str(status_versions()))
        check("回退完成后无残留未决",
              state_read().get("pending") == {}, str(state_read().get("pending")))

        # 最终实时可达性
        vp = run("verify", "--state", STATE)
        check("回退后 21 条逐跳探测全部可达",
              vp.stdout.count(" OK") == 21, head(vp.stdout, 3))

    finally:
        for proc in (p1, p2, p4):
            if proc is not None and proc.poll() is None:
                proc.kill()
                try:
                    proc.wait(timeout=5)
                except Exception:
                    pass
        subprocess.run([sys.executable, "-I", RUNNER, "route_release.launcher",
                        "down", "--workdir", WD], cwd=ROOT,
                       capture_output=True)

    elapsed = time.monotonic() - START
    section(f"结果: {PASS} PASS / {FAIL} FAIL   用时 {elapsed:.1f}s (上限 80s)")
    check("整体在 80 秒内完成", elapsed <= 80.0, f"{elapsed:.1f}s")
    if FAIL == 0:
        shutil.rmtree(WD, ignore_errors=True)
    else:
        print(f"有失败项, 保留现场: {WD}")
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(main())

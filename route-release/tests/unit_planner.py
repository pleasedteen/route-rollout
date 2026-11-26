#!/usr/bin/env python3 -I
"""规划器与模型的纯逻辑单元测试(不起服务, 秒级完成)。

覆盖:
  - 合法/非法配置校验(缺类、未知下一跳、环路、错误本地吸收)
  - 内容指纹的幂等性
  - 规划器满足两项上限
  - 规划安全性: 计划里每个批次的任意部分生效子集都可达
  - 无解原因可读, 放宽上限后有解
  - 回退方向规划
"""
import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                "..", "src"))

from route_release.model import (  # noqa: E402
    Rule, Topology, mixed_assignment_reachable, ruleset_from_plain,
    trace_path, version_token,
)
from route_release.planner import (  # noqa: E402
    _all_subsets, load_ruleset_file, make_plan,
)

EX = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                  "..", "..", "examples"))
FAILED = []


def check(name, cond, detail=""):
    print(("  PASS  " if cond else "  FAIL  ") + name + ("" if cond else f"  {detail}"))
    if not cond:
        FAILED.append(name)


def load(name):
    with open(os.path.join(EX, name), encoding="utf-8") as fh:
        return json.load(fh)


def main():
    topo = Topology.from_dict(load("topology.json"))
    old = load_ruleset_file(os.path.join(EX, "old_rules.json"))
    new = load_ruleset_file(os.path.join(EX, "new_rules.json"))

    print("== 配置校验 ==")
    check("发布前配置合法", topo.validate_rules(old) is None)
    check("目标配置合法", topo.validate_rules(new) is None)
    bad = {n: dict(r) for n, r in old.items()}
    bad["edge1"] = dict(bad["edge1"])
    bad["edge1"]["voice"] = Rule(next_hop="ghost")
    err = topo.validate_rules(bad)
    check("未知下一跳被拦截", err is not None and "ghost" in err, err)
    loop = {n: dict(r) for n, r in old.items()}
    loop["edge1"] = dict(loop["edge1"])
    loop["edge1"]["data"] = Rule(next_hop="edge1")
    err = topo.validate_rules(loop)
    check("自环被拦截", err is not None and "环路" in err, err)

    print("== 指纹幂等 ==")
    r1 = ruleset_from_plain(load("old_rules.json")["edge1"])
    r2 = ruleset_from_plain(load("old_rules.json")["edge1"])
    check("相同规则指纹相同", version_token(r1) == version_token(r2))
    check("不同规则指纹不同",
          version_token(old["edge1"]) != version_token(new["edge1"]))

    print("== 规划与上限 ==")
    plan = make_plan(topo, old, new, 2, 4)
    check("规划成功", plan.ok, plan.reason)
    check("批数不超上限", len(plan.batches) <= 4)
    check("单批规模不超上限", all(len(b) <= 2 for b in plan.batches))
    covered = sorted(n for b in plan.batches for n in b)
    check("每台恰好出现一次", covered == sorted(topo.nodes))

    print("== 任意部分生效都可达(规划安全性) ==")
    prefix = frozenset()
    safe_all = True
    for batch in plan.batches:
        for subset in _all_subsets(batch):
            if mixed_assignment_reachable(topo, old, new,
                                          prefix | subset) is not None:
                safe_all = False
        prefix |= frozenset(batch)
    check("所有批次的所有子集混合状态均可达", safe_all)

    print("== trace_path 与固定出口 ==")
    path, err = trace_path(topo, new, "edge1", "video")
    check("新配置下 edge1 video 无环到达 hub",
          err is None and path[-1] == "hub", str((path, err)))

    print("== 无解与放宽 ==")
    it = Topology.from_dict(load("infeasible_topology.json"))
    io = load_ruleset_file(os.path.join(EX, "infeasible_old.json"))
    inew = load_ruleset_file(os.path.join(EX, "infeasible_new.json"))
    bad_plan = make_plan(it, io, inew, 3, 1)
    check("约束下无解", not bad_plan.ok)
    check("无解原因含批数与环路",
          "至少需要" in bad_plan.reason and "环" in bad_plan.reason,
          bad_plan.reason)
    ok_plan = make_plan(it, io, inew, 3, 3)
    check("放宽后有解", ok_plan.ok)

    print("== 回退方向 ==")
    rb = make_plan(topo, new, old, 2, 4, direction="rollback")
    check("回退规划成功且满足上限",
          rb.ok and len(rb.batches) <= 4
          and all(len(b) <= 2 for b in rb.batches), rb.reason)
    prefix = frozenset()
    rb_safe = True
    for batch in rb.batches:
        for s in _all_subsets(batch):
            if mixed_assignment_reachable(topo, new, old, prefix | s) is not None:
                rb_safe = False
        prefix |= frozenset(batch)
    check("回退任意部分生效也可达", rb_safe)

    print("=" * 56)
    if FAILED:
        print(f"单元测试失败 {len(FAILED)} 项: {FAILED}")
        return 1
    print("单元测试全部通过")
    return 0


if __name__ == "__main__":
    sys.exit(main())

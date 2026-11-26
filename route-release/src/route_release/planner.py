"""批次规划: 在两项上限内找出安全的整批切换顺序。

安全条件(对发布与回退同样适用):
  设批次依次为 B1..Bm。对任意批次 Bi 和它的任意子集 S(含空集与全集),
  "前 i-1 批已整体生效、Bi 中恰好 S 已生效" 的混合状态下,
  从任一节点出发的每一类流量都必须无环并到达其固定出口。

枚举所有子集即可覆盖"同批节点按任意顺序生效、中途只切了一部分"的全部情形。
规划失败时给出可读原因; 规划本身不修改任何服务配置。
"""
from __future__ import annotations

from dataclasses import dataclass
from itertools import combinations
from typing import FrozenSet, List, Mapping, Optional, Sequence, Tuple

from .model import (
    Ruleset,
    Topology,
    mixed_assignment_reachable,
    ruleset_from_plain,
    version_token,
)


@dataclass(frozen=True)
class Plan:
    direction: str                      # "forward" 或 "rollback"
    batches: Tuple[Tuple[str, ...], ...]
    max_batch_size: int
    max_batches: int
    from_tokens: Mapping[str, str]      # 每台服务起始规则集指纹
    to_tokens: Mapping[str, str]        # 每台服务目标规则集指纹
    reason: Optional[str] = None

    @property
    def ok(self) -> bool:
        return self.reason is None

    def to_dict(self) -> dict:
        return {
            "direction": self.direction,
            "batches": [list(b) for b in self.batches],
            "max_batch_size": self.max_batch_size,
            "max_batches": self.max_batches,
            "from_tokens": dict(self.from_tokens),
            "to_tokens": dict(self.to_tokens),
            "reason": self.reason,
        }

    @classmethod
    def from_dict(cls, data: Mapping) -> "Plan":
        return cls(
            direction=data["direction"],
            batches=tuple(tuple(b) for b in data["batches"]),
            max_batch_size=data["max_batch_size"],
            max_batches=data["max_batches"],
            from_tokens=dict(data["from_tokens"]),
            to_tokens=dict(data["to_tokens"]),
            reason=data.get("reason"),
        )


def _all_subsets(nodes: Sequence[str]) -> List[FrozenSet[str]]:
    out: List[FrozenSet[str]] = [frozenset()]
    for r in range(1, len(nodes) + 1):
        for combo in combinations(nodes, r):
            out.append(frozenset(combo))
    return out


def _batch_safe(
    topo: Topology,
    base: Mapping[str, Ruleset],
    target: Mapping[str, Ruleset],
    prefix: FrozenSet[str],
    batch: Sequence[str],
) -> Optional[str]:
    """校验一个批次在任意部分生效子集下都安全。"""
    for subset in _all_subsets(batch):
        err = mixed_assignment_reachable(topo, base, target, prefix | subset)
        if err:
            who = "空集" if not subset else "、".join(sorted(subset))
            return f"批次 {sorted(batch)} 仅生效子集[{who}]时: {err}"
    return None


def _search(
    topo: Topology,
    base: Mapping[str, Ruleset],
    target: Mapping[str, Ruleset],
    max_batch_size: int,
    max_batches: int,
) -> Optional[Tuple[Tuple[str, ...], ...]]:
    nodes = list(topo.nodes)

    def recurse(remaining: List[str],
                batches: Tuple[Tuple[str, ...], ...]
                ) -> Optional[Tuple[Tuple[str, ...], ...]]:
        if not remaining:
            return batches
        # 已在前面批次生效的节点集合, 用于混合状态校验
        prefix = frozenset().union(*batches) if batches else frozenset()
        for node in tuple(remaining):
            rest = [n for n in remaining if n != node]
            # 选项 1: 加入任一未满的现有批次(保持批内有序)
            for i, batch in enumerate(batches):
                if len(batch) >= max_batch_size:
                    continue
                new_batch = batch + (node,)
                batch_prefix = (frozenset().union(*batches[:i])
                                if i else frozenset())
                if _batch_safe(topo, base, target, batch_prefix,
                               new_batch) is not None:
                    continue
                new_batches = batches[:i] + (new_batch,) + batches[i + 1:]
                found = recurse(rest, new_batches)
                if found is not None:
                    return found
            # 选项 2: 开启新批次
            if len(batches) < max_batches:
                if _batch_safe(topo, base, target, prefix, [node]) is None:
                    found = recurse(rest, batches + ((node,),))
                    if found is not None:
                        return found
        return None

    return recurse(nodes, ())


def make_plan(
    topo: Topology,
    base: Mapping[str, Ruleset],
    target: Mapping[str, Ruleset],
    max_batch_size: int,
    max_batches: int,
    direction: str = "forward",
) -> Plan:
    """生成完整计划; 无解时 Plan.ok 为 False 且 reason 说明原因。"""
    from_tokens = {n: version_token(base[n]) for n in topo.nodes}
    to_tokens = {n: version_token(target[n]) for n in topo.nodes}
    if max_batch_size < 1 or max_batches < 1:
        return _fail(direction, max_batch_size, max_batches, from_tokens, to_tokens,
                     "单批服务数上限和总批数上限都必须 >= 1")
    err = topo.validate_rules(base)
    if err:
        return _fail(direction, max_batch_size, max_batches, from_tokens, to_tokens,
                     f"起始配置本身不合法: {err}")
    err = topo.validate_rules(target)
    if err:
        return _fail(direction, max_batch_size, max_batches, from_tokens, to_tokens,
                     f"目标配置本身不合法: {err}")

    n = len(topo.nodes)
    if all(base[x] == target[x] for x in topo.nodes):
        return Plan(direction, (), max_batch_size, max_batches, from_tokens, to_tokens)

    min_batches_by_size = (n + max_batch_size - 1) // max_batch_size
    if min_batches_by_size > max_batches:
        return _fail(
            direction, max_batch_size, max_batches, from_tokens, to_tokens,
            f"{n} 台服务、单批最多 {max_batch_size} 台, 至少需要 "
            f"{min_batches_by_size} 批, 超过总批数上限 {max_batches}",
        )

    found = _search(topo, base, target, max_batch_size, max_batches)
    if found is not None:
        return Plan(direction, found, max_batch_size, max_batches,
                    from_tokens, to_tokens)

    # 放松总批数上限求最少批数, 给出可操作的无解原因
    relaxed = _search(topo, base, target, max_batch_size, n)
    detail = _blocking_detail(topo, base, target)
    if relaxed is not None:
        reason = (
            f"在单批不超过 {max_batch_size} 台、且任意部分生效都保持可达的约束下, "
            f"至少需要 {len(relaxed)} 批(建议顺序: "
            f"{[list(b) for b in relaxed]}), 超过总批数上限 {max_batches}。"
            f"根因示例: {detail}"
        )
    else:  # 理论上不会发生: 单节点成批时两个合法配置之间恒可逐台切换
        reason = "不存在满足任意部分生效均可达的批次划分, 请检查两套配置"
    return _fail(direction, max_batch_size, max_batches, from_tokens, to_tokens, reason)


def _blocking_detail(topo: Topology, base: Mapping[str, Ruleset],
                     target: Mapping[str, Ruleset]) -> str:
    """找出一个具体的可达性破坏点(如环路), 附在无解原因里便于值班人员判断。"""
    # 优先报告"全部塞进一批"时第一个出错的部分生效子集
    err = _batch_safe(topo, base, target, frozenset(), list(topo.nodes))
    if err:
        return err
    # 否则报告第一个无法单独切换的节点
    for node in topo.nodes:
        err = _batch_safe(topo, base, target, frozenset(), [node])
        if err:
            return err
    return "部分节点同时切换会破坏可达性"


def _fail(direction, max_batch_size, max_batches, from_tokens, to_tokens,
          reason) -> Plan:
    return Plan(direction, (), max_batch_size, max_batches,
                from_tokens, to_tokens, reason)


def load_ruleset_file(path: str) -> Mapping[str, Ruleset]:
    """读取 {node: {class: rule}} 形式的配置文件。"""
    import json

    with open(path, "r", encoding="utf-8") as fh:
        raw = json.load(fh)
    return {node: ruleset_from_plain(rules) for node, rules in raw.items()}

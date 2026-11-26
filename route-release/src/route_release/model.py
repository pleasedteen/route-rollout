"""数据模型与路由可达性校验。

一台转发服务(节点)对每个流量类别保存一条下一跳规则:
  - 若下一跳是另一台服务, 规则为 {"next_hop": "<节点名>"}
  - 若本节点就是该类流量的固定出口, 规则为 {"local_egress": true}

每台服务的整套规则就是 {class_name: rule}; 规则集的内容指纹(version token)
随状态落盘, 发布工具用它判断服务上"实际生效的版本"。
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Dict, FrozenSet, Mapping, Optional, Tuple


@dataclass(frozen=True)
class Rule:
    """单个流量类别的下一跳规则。"""

    next_hop: Optional[str] = None
    local_egress: bool = False

    def to_dict(self) -> dict:
        if self.local_egress:
            return {"local_egress": True}
        return {"next_hop": self.next_hop}

    @classmethod
    def from_dict(cls, data: Mapping) -> "Rule":
        if data.get("local_egress"):
            return cls(local_egress=True)
        nh = data.get("next_hop")
        if not nh:
            raise ValueError(f"非法规则(既无 next_hop 也不是 local_egress): {data!r}")
        return cls(next_hop=nh)


Ruleset = Dict[str, Rule]


def ruleset_to_plain(rules: Mapping[str, Rule]) -> Dict[str, dict]:
    return {cls: r.to_dict() for cls, r in sorted(rules.items())}


def ruleset_from_plain(data: Mapping[str, Mapping]) -> Ruleset:
    return {cls: Rule.from_dict(rule) for cls, rule in data.items()}


def version_token(rules: Mapping[str, Rule]) -> str:
    """规则集的内容指纹; 内容相同则 token 相同(幂等 apply 的判定依据)。"""
    payload = json.dumps(ruleset_to_plain(rules), sort_keys=True, separators=(",", ":"))
    return "v_" + hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]


@dataclass(frozen=True)
class Topology:
    """静态拓扑: 节点集合、流量类别集合、每类流量的固定出口。"""

    nodes: Tuple[str, ...]
    classes: Tuple[str, ...]
    egress_of: Mapping[str, str]  # class -> 固定出口节点

    @classmethod
    def from_dict(cls, data: Mapping) -> "Topology":
        nodes = tuple(data["nodes"])
        classes = tuple(data["classes"])
        egress_map = dict(data["egress"])
        dup = len(set(nodes)) != len(nodes) or len(set(classes)) != len(classes)
        if dup:
            raise ValueError("节点名或流量类别名存在重复")
        for cls_name, eg in egress_map.items():
            if cls_name not in classes:
                raise ValueError(f"出口表引用了未知流量类别: {cls_name}")
            if eg not in nodes:
                raise ValueError(f"类别 {cls_name} 的固定出口 {eg} 不是已知节点")
        missing = [c for c in classes if c not in egress_map]
        if missing:
            raise ValueError(f"以下流量类别缺少固定出口: {missing}")
        return cls(nodes, classes, egress_map)

    def validate_rules(self, rules: Mapping[str, Ruleset]) -> Optional[str]:
        """校验一套完整配置的静态合法性与可达性, 返回 None 或错误原因。"""
        for node in self.nodes:
            if node not in rules:
                return f"节点 {node} 缺少整套规则"
            rs = rules[node]
            if set(rs) != set(self.classes):
                return f"节点 {node} 的流量类别集合应为 {sorted(self.classes)}, 实际为 {sorted(rs)}"
            for cls, rule in rs.items():
                if not rule.local_egress and rule.next_hop not in self.nodes:
                    return f"节点 {node} 类别 {cls} 的下一跳 {rule.next_hop} 不存在"
        # 逐类别沿下一跳模拟, 必须无环且到达各自固定出口
        for cls in self.classes:
            egress = self.egress_of[cls]
            for start in self.nodes:
                seen = set()
                cur = start
                while True:
                    rule = rules[cur][cls]
                    if rule.local_egress:
                        if cur != egress:
                            return (
                                f"类别 {cls}: 节点 {start} 的流量在 {cur} 被本地吸收, "
                                f"但固定出口应为 {egress}"
                            )
                        break
                    if cur in seen:
                        chain = " -> ".join(sorted(seen)) + f" -> {cur}"
                        return f"类别 {cls}: 从 {start} 出发存在转发环路({chain})"
                    seen.add(cur)
                    cur = rule.next_hop  # type: ignore[assignment]
        return None


def mixed_assignment_reachable(
    topo: Topology,
    old: Mapping[str, Ruleset],
    new: Mapping[str, Ruleset],
    on_new: frozenset,
) -> Optional[str]:
    """校验"部分节点已切新规则、其余仍是旧规则"的混合状态。

    从任一节点、任一类流量出发, 按各节点当前生效的规则转发, 必须无环并到达
    该类别的固定出口。返回 None 表示可达, 否则返回具体原因。
    """
    merged: Dict[str, Ruleset] = {}
    for node in topo.nodes:
        merged[node] = new[node] if node in on_new else old[node]
    return topo.validate_rules(merged)


def trace_path(
    topo: Topology, rules: Mapping[str, Ruleset], start: str, cls: str
) -> Tuple[list, Optional[str]]:
    """按给定规则模拟一条流量路径, 返回 (路径节点列表, 错误原因)。"""
    path = [start]
    seen = {start}
    cur = start
    while True:
        rule = rules[cur][cls]
        if rule.local_egress:
            err = None if cur == topo.egress_of[cls] else (
                f"在 {cur} 被本地吸收, 但固定出口应为 {topo.egress_of[cls]}"
            )
            return path, err
        nxt = rule.next_hop
        if nxt not in topo.nodes:
            return path, f"下一跳 {nxt} 不存在"
        if nxt in seen:
            path.append(nxt)
            return path, f"转发环路: {' -> '.join(path)}"
        seen.add(nxt)
        path.append(nxt)
        cur = nxt  # type: ignore[assignment]


@dataclass(frozen=True)
class Migration:
    """一次固定出口迁移: 把 cls 类流量的固定出口 old_egress 迁到 new_egress。"""

    cls: str
    old_egress: str
    new_egress: str

    @classmethod
    def from_dict(cls, data: Mapping) -> "Migration":
        return cls(cls=data["class"], old_egress=data["old_egress"],
                   new_egress=data["new_egress"])

    def to_dict(self) -> dict:
        return {"class": self.cls, "old_egress": self.old_egress,
                "new_egress": self.new_egress}


def migration_rules_valid(topo: Topology, rules: Mapping[str, Ruleset],
                          mig: Migration) -> Optional[str]:
    """校验迁移场景下的一套配置。

    对迁移类别: 本地吸收只允许发生在旧/新出口之一, 其余节点必须无环并最终
    走到其中一个出口(迁移过程中两套配置的静态出口表不同, 不能用旧拓扑表硬卡)。
    对其他类别: 仍按拓扑里的固定出口严格校验。
    """
    allowed_egress = {mig.old_egress, mig.new_egress}
    for cls in topo.classes:
        static_egress = topo.egress_of[cls]
        for start in topo.nodes:
            seen: set = set()
            cur = start
            while True:
                rule = rules[cur][cls]
                if rule.local_egress:
                    if cls == mig.cls:
                        if cur not in allowed_egress:
                            return (
                                f"类别 {cls}: 节点 {start} 的流量在 {cur} 被本地吸收, "
                                f"迁移期间只允许在出口 {sorted(allowed_egress)} 吸收"
                            )
                    elif cur != static_egress:
                        return (
                            f"类别 {cls}: 节点 {start} 的流量在 {cur} 被本地吸收, "
                            f"但固定出口应为 {static_egress}"
                        )
                    break
                if cur in seen:
                    chain = " -> ".join(sorted(seen)) + f" -> {cur}"
                    return f"类别 {cls}: 从 {start} 出发存在转发环路({chain})"
                seen.add(cur)
                nxt = rule.next_hop
                if nxt not in topo.nodes:
                    return f"类别 {cls}: 节点 {cur} 的下一跳 {nxt} 不存在"
                cur = nxt  # type: ignore[assignment]
    return None


def migration_mixed_reachable(
    topo: Topology,
    old: Mapping[str, Ruleset],
    new: Mapping[str, Ruleset],
    on_new: FrozenSet[str],
    mig: Migration,
) -> Optional[str]:
    """迁移期间的混合状态可达性校验。

    部分节点已切新规则、其余仍是旧规则时, 迁移类别允许流量被旧出口或新出口
    吸收(粘性路由保证任一跳都能找到对应版本的规则), 其他类别仍按拓扑出口。
    """
    merged: Dict[str, Ruleset] = {}
    for node in topo.nodes:
        merged[node] = new[node] if node in on_new else old[node]
    return migration_rules_valid(topo, merged, mig)


def migration_target_gate(topo: Topology,
                          target: Mapping[str, Ruleset],
                          mig: Migration) -> Optional[str]:
    """切入口前的目标规则校验(迁移专用), 不过则保持现网配置。

    - 新出口在目标配置中必须成为迁移类别的本地固定出口;
    - 旧出口在目标配置中必须不再本地吸收迁移类别(改成转发);
    - 入口节点在目标配置中不得变成该类别的出口。
    """
    if not target[mig.new_egress][mig.cls].local_egress:
        return (f"目标规则校验不过: 新出口 {mig.new_egress} 对类别 {mig.cls} "
                f"不是 local_egress, 无法承接固定出口")
    if target[mig.old_egress][mig.cls].local_egress:
        return (f"目标规则校验不过: 旧出口 {mig.old_egress} 在目标配置中仍本地吸收 "
                f"类别 {mig.cls}, 迁移后无法停用旧出口接收")
    for node in topo.nodes:
        if node in (mig.old_egress, mig.new_egress):
            continue
        if target[node][mig.cls].local_egress:
            return (f"目标规则校验不过: 非出口节点 {node} 不得成为类别 {mig.cls} "
                    f"的本地出口")
    return None

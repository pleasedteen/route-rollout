"""发布进程的持久化状态(原子写 + 跨进程文件锁)。

新进程只凭这个文件 + 各服务 /status 的实际生效版本就能接手,
不依赖旧进程的内存。状态机:

  RUNNING          正向发布进行中
  PAUSED           暂停已接受: 不再开启任何新操作, 在途操作待出结果
  FORWARD_COMPLETE 正向发布全部批次已生效
  ROLLING_BACK     回退进行中
  ROLLED_BACK      已全部恢复到发布前配置
  FAILED           服务连续不可用等致命错误(配置未被破坏, 可继续/回退)

字段说明:
  epoch     单调递增的栅栏代数。每次下发 apply/fence 都从它分配更大的值;
            服务端记录见过的最大 epoch, 拒绝更小 epoch 的写入。这样任何
            "迟到/重发"的旧请求都不会覆盖暂停或回退后的结果。
  pending   已发出但尚未确认生效的操作: node -> {epoch,target,direction,batch,
            started_at}。在发 HTTP 之前落盘, 在确认生效或确认不会生效后删除。
            进程崩溃后新进程据此接手未决操作。

所有写操作都在跨进程 flock 下"读-改-写", 因此并行的多个 CLI 进程不会
互相覆盖, 且"暂停已接受"与"能否开新批"的判断是原子的。
"""
from __future__ import annotations

import fcntl
import json
import os
from contextlib import contextmanager
from typing import Any, Callable, Dict, Optional

from .planner import Plan

PHASES = ("RUNNING", "PAUSED", "FORWARD_COMPLETE",
          "ROLLING_BACK", "ROLLED_BACK", "FAILED")


class ReleaseStore:
    def __init__(self, path: str):
        self.path = path
        self.lock_path = path + ".lock"

    def exists(self) -> bool:
        return os.path.exists(self.path)

    @contextmanager
    def _exclusive(self):
        d = os.path.dirname(os.path.abspath(self.path))
        os.makedirs(d, exist_ok=True)
        fd = os.open(self.lock_path, os.O_CREAT | os.O_RDWR, 0o644)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX)
            yield
        finally:
            fcntl.flock(fd, fcntl.LOCK_UN)
            os.close(fd)

    @staticmethod
    def _normalize(data: Dict[str, Any]) -> Dict[str, Any]:
        data.setdefault("epoch", 0)
        data.setdefault("pending", {})
        data.setdefault("applied", {})
        return data

    def _read_raw(self) -> Dict[str, Any]:
        with open(self.path, "r", encoding="utf-8") as fh:
            return self._normalize(json.load(fh))

    def _write_raw(self, data: Dict[str, Any]) -> None:
        os.makedirs(os.path.dirname(os.path.abspath(self.path)), exist_ok=True)
        tmp = self.path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump(data, fh, ensure_ascii=False, indent=2)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, self.path)

    def read(self) -> Dict[str, Any]:
        # 读取不加锁: 写入走 "临时文件 + os.replace", 读者只会看到完整的旧/新文件。
        return self._read_raw()

    def create(self, *, release_id: str, topology_path: str,
               old_rules_path: str, new_rules_path: str,
               plan: Plan, registry_path: str) -> None:
        with self._exclusive():
            if self.exists():
                raise SystemExit(f"发布状态已存在: {self.path}, "
                                 f"如需重新发布请先 rollback 或删除状态文件")
            self._write_raw({
                "release_id": release_id,
                "phase": "RUNNING",
                "topology_path": topology_path,
                "old_rules_path": old_rules_path,
                "new_rules_path": new_rules_path,
                "registry_path": registry_path,
                "forward_plan": plan.to_dict(),
                "rollback_plan": None,
                "current_batch_index": 0,   # 正在进行的批次(从 0 开始)
                "direction": "forward",
                "applied": {},              # node -> 已确认生效的版本 token
                "epoch": 0,                 # 栅栏代数(单调递增)
                "pause_epoch": None,        # 接受暂停时用的栅栏代数
                "pending": {},              # node -> 已发出未确认的操作
                "last_error": None,
            })

    def update(self, **fields) -> Dict[str, Any]:
        with self._exclusive():
            data = self._read_raw()
            unknown = set(fields) - set(data)
            if unknown:
                raise KeyError(f"未知状态字段: {sorted(unknown)}")
            data.update(fields)
            self._write_raw(data)
            return data

    def mutate(self, fn: Callable[[Dict[str, Any]], Optional[Dict[str, Any]]]
               ) -> Dict[str, Any]:
        """在跨进程排他锁下读-改-写。fn 可原地修改并返回 None, 或返回新字典。"""
        with self._exclusive():
            data = self._read_raw()
            result = fn(data)
            if result is not None:
                data = result
            self._write_raw(data)
            return data

    def mutate_cond(self, fn: Callable[[Dict[str, Any]], bool]
                    ) -> tuple[bool, Dict[str, Any]]:
        """在跨进程排他锁下条件更新: fn 返回 True 才落盘, 否则原样返回。

        用于"只在阶段仍是 X 时才推进"这类不能与另一进程竞争的转换。
        """
        with self._exclusive():
            data = self._read_raw()
            commit = fn(data)
            if commit:
                self._write_raw(data)
            return commit, data

    def alloc_epoch(self, min_epoch: int = 0) -> int:
        """分配一个严格更大的栅栏代数(跨进程安全)。

        min_epoch 是服务端当前观测到的最大栅栏: 新代数必须超过它, 否则新一次
        发布分配的代数会被旧发布留在服务上的栅栏永久拒止。
        """
        box: Dict[str, int] = {}

        def fn(data):
            epoch = max(int(data.get("epoch", 0)), int(min_epoch)) + 1
            data["epoch"] = epoch
            box["epoch"] = epoch

        self.mutate(fn)
        return box["epoch"]

    def accept_pause(self, min_epoch: int = 0) -> tuple[Optional[int], str]:
        """原子地接受暂停: 置 PAUSED 并抬高代数。

        返回 (栅栏代数, 结果)。结果:
          accepted        本次接受, 用返回的代数给所有节点装栅栏
          already         之前已接受(可能是另一个 pause 进程), 复用其代数
          not_applicable  当前阶段无需/不允许暂停
        与 begin_op 共用同一把排他锁, 因此"接受暂停"之后任何进程都无法再
        登记新的操作, 也就无法开启新批次。
        """
        box: Dict[str, object] = {}

        def fn(data):
            phase = data["phase"]
            if phase == "PAUSED":
                pe = data.get("pause_epoch")
                if pe is None:  # 兼容旧状态文件
                    pe = max(int(data.get("epoch", 0)), int(min_epoch)) + 1
                    data["epoch"] = pe
                    data["pause_epoch"] = pe
                    return True
                box["epoch"] = int(pe)
                box["result"] = "already"
                return False
            if phase not in ("RUNNING", "FAILED", "ROLLING_BACK"):
                box["result"] = "not_applicable"
                return False
            epoch = max(int(data.get("epoch", 0)), int(min_epoch)) + 1
            data["epoch"] = epoch
            data["pause_epoch"] = epoch
            data["phase"] = "PAUSED"
            box["epoch"] = epoch
            box["result"] = "accepted"
            return True

        self.mutate_cond(fn)
        return box.get("epoch"), str(box.get("result"))

    def begin_op(self, node: str, *, direction: str, batch, target: str,
                 started_at: str, min_epoch: int = 0) -> Optional[int]:
        """登记一次即将发出的操作, 返回其栅栏代数; 若当前不允许下发则返回 None。

        在发 HTTP 之前落盘 pending, 所以进程在任何时刻崩溃, 新进程都能看到
        "已发出但未确认"的操作。栅栏代数单调递增, 重试时自然覆盖旧请求。
        """
        box: Dict[str, Optional[int]] = {"epoch": None}

        def fn(data):
            if data["phase"] not in ("RUNNING", "ROLLING_BACK", "FAILED"):
                return
            if data["direction"] != direction:
                return
            epoch = max(int(data.get("epoch", 0)), int(min_epoch)) + 1
            data["epoch"] = epoch
            data.setdefault("pending", {})[node] = {
                "epoch": epoch,
                "direction": direction,
                "batch": list(batch),
                "target": target,
                "started_at": started_at,
            }
            box["epoch"] = epoch

        self.mutate(fn)
        return box["epoch"]

    def finish_op(self, node: str, epoch: int, *, effective: bool,
                  version: Optional[str] = None) -> None:
        """结束一次操作: 确认生效或确认不会生效; 只清理同一代数的 pending。"""
        def fn(data):
            pending = data.get("pending", {})
            entry = pending.get(node)
            if entry and int(entry.get("epoch", -1)) == epoch:
                if effective and version:
                    data["applied"][node] = version
                del pending[node]

        self.mutate(fn)

    def mark_applied(self, node: str, version: str) -> None:
        def fn(data):
            data["applied"][node] = version
        self.mutate(fn)

    def observe(self, node: str, version: str) -> None:
        """记录一次实际观测: 若该节点未决操作的目标正是此版本, 按实际结果收尾。"""
        def fn(data):
            data["applied"][node] = version
            entry = data.setdefault("pending", {}).get(node)
            if entry is not None and entry.get("target") == version:
                del data["pending"][node]
        self.mutate(fn)

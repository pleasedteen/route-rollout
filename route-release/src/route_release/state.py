"""发布进程的持久化状态(原子写)。

新进程只凭这个文件 + 各服务 /status 的实际生效版本就能接手,
不依赖旧进程的内存。状态机:

  RUNNING          正向发布进行中
  PAUSED           暂停: 不再开启新批次, 在途操作已有明确结果
  FORWARD_COMPLETE 正向发布全部批次已生效
  ROLLING_BACK     回退进行中
  ROLLED_BACK      已全部恢复到发布前配置
  FAILED           服务连续不可用等致命错误(配置未被破坏, 可继续/回退)
"""
from __future__ import annotations

import json
import os
import threading
from typing import Any, Dict, Optional

from .planner import Plan

PHASES = ("RUNNING", "PAUSED", "FORWARD_COMPLETE", "DRAINING",
          "DECOMMISSIONED", "ROLLING_BACK", "ROLLED_BACK", "FAILED")


class ReleaseStore:
    def __init__(self, path: str):
        self.path = path
        self.lock = threading.Lock()

    def exists(self) -> bool:
        return os.path.exists(self.path)

    def write(self, data: Dict[str, Any]) -> None:
        os.makedirs(os.path.dirname(os.path.abspath(self.path)), exist_ok=True)
        tmp = self.path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump(data, fh, ensure_ascii=False, indent=2)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, self.path)

    def read(self) -> Dict[str, Any]:
        with open(self.path, "r", encoding="utf-8") as fh:
            return json.load(fh)

    def create(self, *, release_id: str, topology_path: str,
               old_rules_path: str, new_rules_path: str,
               plan: Plan, registry_path: str,
               migration: Optional[dict] = None) -> None:
        with self.lock:
            if self.exists():
                raise SystemExit(f"发布状态已存在: {self.path}, "
                                 f"如需重新发布请先 rollback 或删除状态文件")
            self.write({
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
                "last_error": None,
                # 固定出口迁移专用(普通发布为 None)
                "migration": migration,
                "decommission": None,       # 停用旧出口的结果记录
            })

    def update(self, **fields) -> Dict[str, Any]:
        with self.lock:
            data = self.read()
            unknown = set(fields) - set(data)
            if unknown:
                raise KeyError(f"未知状态字段: {sorted(unknown)}")
            data.update(fields)
            self.write(data)
            return data

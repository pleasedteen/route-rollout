#!/usr/bin/env bash
# 一条命令跑完固定出口迁移演练(目标校验/部分切换/跨切换请求/接手停用/回退)。
# 实测约 20-40 秒, 要求 80 秒内完成。仅依赖 Python 3.11 标准库。
set -euo pipefail
cd "$(dirname "$0")"
python3 -I tests/e2e_migration.py

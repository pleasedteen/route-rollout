#!/usr/bin/env bash
# 一条命令跑完全部本地测试: 先纯逻辑单元测试(秒级), 再端到端故障演练(约 10-25 秒)。
# 正常整体 15-30 秒, 远低于 80 秒。仅依赖 Python 3.11 标准库。
set -euo pipefail
cd "$(dirname "$0")"

echo "########## 第一部分: 规划器/模型单元测试 ##########"
python3 -I tests/unit_planner.py

echo
echo "########## 第二部分: 端到端故障演练(真实本地进程) ##########"
python3 -I tests/e2e_demo.py

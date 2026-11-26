#!/usr/bin/env bash
# 一条命令跑完所有测试: 单元逻辑 + 普通发布端到端 + 固定出口迁移端到端。
# 实测约 35-55 秒, 要求 80 秒内完成。仅依赖 Python 3.11 标准库。
set -euo pipefail
cd "$(dirname "$0")"

echo "########## 第一部分: 规划器/模型单元测试(含迁移规划) ##########"
python3 -I tests/unit_planner.py

echo
echo "########## 第二部分: 普通批量发布端到端故障演练 ##########"
python3 -I tests/e2e_demo.py

echo
echo "########## 第三部分: 固定出口迁移端到端(部分切换/跨切换/接手/回退) ##########"
python3 -I tests/e2e_migration.py

#!/usr/bin/env bash
# 一条命令跑完全部本地测试(要求 80 秒内):
#   1) 规划器/模型单元测试(秒级)
#   2) 安全语义验收: 延迟投递/丢确认/进程接手/暂停与回退(真实本地进程, ~15s)
#   3) 端到端故障演练(真实本地进程, ~15s)
# 仅依赖 Python 3.11 标准库。任一部分失败即整体非零退出。
set -euo pipefail
cd "$(dirname "$0")"

echo "########## 第一部分: 规划器/模型单元测试 ##########"
python3 -I tests/unit_planner.py

echo
echo "########## 第二部分: 安全语义验收(延迟投递/丢确认/进程接手/暂停/回退) ##########"
python3 -I tests/acceptance.py

echo
echo "########## 第三部分: 端到端故障演练(真实本地进程) ##########"
python3 -I tests/e2e_demo.py

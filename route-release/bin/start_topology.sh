#!/usr/bin/env bash
# 手动演示: 拉起拓扑(edge2/edge4 带故障代理), 之后可用 bin/pub 执行发布命令。
set -euo pipefail
cd "$(dirname "$0")/.."
WD="${1:-./.manual-workdir}"
python3 -I bin/runmod.py route_release.launcher up \
  --workdir "$WD" \
  --topology ../examples/topology.json \
  --initial-rules ../examples/old_rules.json \
  --proxy-node edge2 --proxy-node edge4
echo "工作目录: $WD"
echo "查状态:   bin/pub $WD status"
echo "停服务:   python3 -I bin/runmod.py route_release.launcher down --workdir $WD"

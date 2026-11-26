#!/usr/bin/env python3 -I
"""隔离模式下的模块引导器。

python3 -I 隐含 -P, 不会把当前目录加入 sys.path, 无法直接 -m 本地包。
本引导先把 ../src 注入 sys.path, 再以 __main__ 方式运行目标模块:

    python3 -I bin/runmod.py route_release.service --node edge1 ...
"""
import os
import runpy
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                "..", "src"))
module = sys.argv[1]
sys.argv = [module] + sys.argv[2:]
runpy.run_module(module, run_name="__main__", alter_sys=True)

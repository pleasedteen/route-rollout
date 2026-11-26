#!/usr/bin/env python3 -I
"""发布工具启动器: 注入 src 路径后转交 route_release.publisher.main。

这样既保持隔离模式(-I, 不读环境变量/当前目录), 又不需要安装包。
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                "..", "src"))
from route_release.publisher import main  # noqa: E402

if __name__ == "__main__":
    main()

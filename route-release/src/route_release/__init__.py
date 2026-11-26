"""本地转发服务的安全批量发布工具。

模块组成:
  model    拓扑 / 流量类别 / 规则的数据模型与可达性校验
  planner  满足单批规模上限和总批数上限的批次规划(发布与回退共用)
  service  可独立运行的本地转发服务(带持久化的 HTTP 控制面)
  proxy    故障夹具: 丢确认 / 丢请求 / 转发抖动
  state    发布进程的持久化状态
  publisher 发布工具 CLI(规划、发布、暂停、继续、回退、查状态)
"""

__all__ = ["model", "planner", "service", "proxy", "state", "publisher"]

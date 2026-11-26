# 本地转发服务的安全批量发布工具

在维护窗口里给一组**本地转发服务**更新路由。同一批可能只切成功几台，发布进程也
可能在服务已经切换后丢掉确认。本项目提供：

- 可独立运行、重启后保留生效配置的**本地转发服务**（标准库 HTTP 控制面）；
- 一个**发布工具**：先出满足约束的完整计划，再分批切换，支持查状态、暂停、继续、
  回退、进程崩溃后由新进程接手；
- **故障夹具代理**：复现部分生效、确认丢失、丢请求、抖动；
- 本地拓扑样例与一条 80 秒内跑完的测试命令（实测约 16 秒）。

只依赖 Python 3.11 标准库，无需安装第三方包。所有进程都以 `python3 -I`（隔离模式，
不读当前目录/环境变量，避免被同目录恶意模块劫持）启动。

---

## 1. 目录结构

```
route-release/
├── src/route_release/
│   ├── model.py       规则/拓扑数据模型, 内容指纹, 混合状态可达性校验
│   ├── planner.py     满足单批规模与总批数上限的批次规划(回溯 + 子集枚举)
│   ├── service.py     可独立运行的转发服务(原子持久化 + HTTP 控制面 + 逐跳探测)
│   ├── proxy.py       故障夹具代理(丢确认/丢请求/抖动, 可在线切换)
│   ├── state.py       发布进程的持久化状态(原子写)
│   ├── publisher.py   发布工具 CLI: submit/run/pause/resume/rollback/status/verify
│   └── launcher.py    一键拉起/关闭整套本地拓扑
├── bin/
│   ├── runmod.py      隔离模式下的模块引导器(-I 时 cwd 不进 sys.path)
│   ├── pub / pub.py   发布工具薄封装
│   └── start_topology.sh
├── examples/          拓扑与发布前/目标配置样例, 含一个无解样例
├── tests/
│   ├── unit_planner.py  纯逻辑单元测试
│   └── e2e_demo.py      端到端故障演练
└── run_demo.sh        一条命令跑全部测试
```

## 2. 核心模型与安全语义

一台服务对每个**流量类别**保存一条下一跳规则：

- 转发到下游服务：`{"next_hop": "relay1"}`
- 本节点就是该类流量的**固定出口**：`{"local_egress": true}`

整套规则 `{class: rule}` 经规范化后算 SHA-256 得到**版本指纹**（`version_token`）。
规则内容相同则指纹相同——这是 apply 幂等和"按实际生效版本对账"的基础。

### 规划的安全定义（发布与回退共用）

设批次依次为 B1..Bm。对任意批次 Bi 和它的**任意子集 S（含空集与全集）**，
"前 i-1 批已整体生效、Bi 中恰好 S 已生效"的混合状态下，从任一节点、任一类流量
出发，都必须无环并到达该类别的固定出口。规划器枚举每个批次的全部子集逐一校验，
因此**同批服务按任意顺序生效、中途只切了一部分，路由始终可达**。

规划器用回溯搜索满足 `单批服务数 ≤ max_batch_size`、`总批数 ≤ max_batches`
的划分；无解时放松批数上限求出最少批数，并附一个具体的可达性破坏点（如环路）
作为可读原因，**此时不创建任何发布状态、不触碰服务**。

### 进度与权威状态

发布进程**不持有权威状态**。权威状态只有两处：

1. 状态文件 `release.json`（原子写）：阶段、批次进度、计划、已观察到的版本；
2. 各服务 `/status` 返回的**实际生效版本**（服务本地原子持久化）。

每个推进周期先 `reconcile`：查所有服务的实际版本并据此纠正本地进度，再决定是否
下发。因此即使上一个进程崩溃、确认丢失，新进程只凭状态文件 + 服务实际版本就能
正确接手。apply 按规则指纹幂等，重发安全。

阶段机：`RUNNING → PAUSED → FORWARD_COMPLETE → ROLLING_BACK → ROLLED_BACK`，
连续不可达时进入 `FAILED`（配置不被破坏，恢复后 `resume` 即可）。

---

## 3. 快速开始

### 一条命令跑全部测试（约 16 秒，要求 80 秒内）

```bash
cd route-release
bash run_demo.sh
```

输出形如：

```
########## 第一部分: 规划器/模型单元测试 ##########
  PASS  发布前配置合法
  ...
单元测试全部通过
########## 第二部分: 端到端故障演练(真实本地进程) ##########
场景 0: 无解规划 ...
场景 3: 部分生效——发布进程在批次中途被 kill -9
场景 4: 新进程接手, 先对账再续跑(确认丢失场景随后注入)
...
结果: 22 PASS / 0 FAIL
```

端到端演练覆盖：无解规划、部分生效后 `kill -9`、新进程接手、确认丢失对账、
逐跳可达性验证、暂停/继续、服务重启保留配置、完成后回退。

### 手动拉起拓扑

```bash
# 拉起 7 台服务; edge2/edge4 额外挂故障代理
bash bin/start_topology.sh .manual-workdir

# 用完清理
python3 -I bin/runmod.py route_release.launcher down --workdir .manual-workdir
```

工作目录下生成：`registry.json`（节点→控制面 URL）、`state/`（各服务持久化规则）、
`proxy/`（故障配置，可在线改写）、`logs/`、`pids.json`。

---

## 4. 发布操作手册

以下用 `WD=.manual-workdir` 表示拓扑工作目录。

### 4.1 提交计划（动配置前先看完整计划）

```bash
python3 -I bin/runmod.py route_release.publisher submit \
  --topology ../examples/topology.json \
  --old-rules ../examples/old_rules.json \
  --new-rules ../examples/new_rules.json \
  --registry $WD/registry.json \
  --state $WD/release.json \
  --max-batch-size 2 --max-batches 4
```

先打印完整批次计划与上限核对结果，再落状态文件。无解时退出码 2 并说明原因，
原配置保留。

### 4.2 发布 / 继续 / 暂停

```bash
python3 -I bin/runmod.py route_release.publisher run    --state $WD/release.json
python3 -I bin/runmod.py route_release.publisher pause  --state $WD/release.json
python3 -I bin/runmod.py route_release.publisher resume --state $WD/release.json
```

- `run` 可反复执行；进程崩溃后直接再次 `run` 或 `resume`，会先对账再续跑。
- `pause` 立即停止开启新批次，等已发出操作的结果明确（实际版本收敛到已知的
  新/老指纹）后保留进度返回。
- `resume` 从保留的进度继续；`FAILED` 也用它恢复。

### 4.3 查状态与实时验证

```bash
python3 -I bin/runmod.py route_release.publisher status --state $WD/release.json
python3 -I bin/runmod.py route_release.publisher verify --state $WD/release.json
```

`status` 列出每台服务的**实际生效版本**、发布前/目标指纹、批次进度。
`verify` 从每个节点、每类流量发起**真实逐跳转发探测**（服务间互相 HTTP 调用，
带环路检测），确认都到达固定出口。

### 4.4 回退到发布前配置

暂停后或发布完成后均可回退：

```bash
python3 -I bin/runmod.py route_release.publisher rollback --state $WD/release.json
```

回退会**以服务当前实际生效配置（可能是新老混合）为起点重新规划**，同样枚举
任意部分生效子集的可达性、受单批规模与总批数上限约束；无解则不动配置并说明原因。
默认沿用正向发布的两项上限，也可用 `--max-batch-size/--max-batches` 覆盖。

---

## 5. 转发服务接口

每台服务独立监听一个本地端口（注册表 `registry.json`）：

| 方法 | 路径 | 作用 |
| --- | --- | --- |
| GET  | `/status`   | 返回节点名、生效版本指纹、各类流量下一跳 |
| POST | `/apply`    | 整套切换本机规则；内容相同则幂等返回 `duplicate:true` |
| POST | `/probe`    | 从本节点逐跳真实转发某类流量，返回完整路径与出口 |
| POST | `/shutdown` | 测试/演示收尾 |

生效规则通过"临时文件 + `os.replace`"原子落盘到 `state/<node>.json`，
进程重启后自动恢复（见端到端场景 7）。

---

## 6. 故障夹具

代理配置在 `$WD/proxy/<node>.json`，**在线改写即生效**（代理按文件 mtime 热加载）。
故障只作用于变更操作 `POST /apply`；`/status`、`/probe` 始终放行——这正是
"确认丢失但仍能查到真实生效状态"的前提。

| mode | 行为 |
| --- | --- |
| `pass` | 全放行 |
| `drop_confirm` | 转发给服务并执行成功，但吞掉响应（发布方连接被断开） |
| `drop_request` | 直接丢弃请求，服务根本不知道这次切换 |
| `flaky` | 前 `fail_times` 次返回 503，之后自愈 |

所有模式都遵守 `fail_times` 配额（注入这么多次后自动恢复）。发布工具对 apply
做有限退避重试，每次重试前先查实际版本，借助幂等保证不重复破坏。

把某台服务接入代理：启动拓扑时加 `--proxy-node <node>`（可重复）。

---

## 7. 关键设计取舍

- **为什么枚举子集而不是只校验整批前后两个状态？** 同一批只切成功几台是常态，
  只校验端点状态无法发现"切了一半成环"。枚举全部子集才能保证任意顺序、任意
  部分生效都可达。
- **为什么 apply 要幂等、且以服务实际版本为准？** 确认丢失时发布方无法区分
  "没生效"和"生效了但没回执"。内容指纹让重发无副作用，对账让进度以服务为准，
  新进程才能无损接手。
- **为什么回退要基于实际混合状态重新规划？** 暂停/崩溃后各节点版本可能新老不一，
  从纯目标配置规划出的批次未必覆盖真实的混合起点。

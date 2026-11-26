# 本地转发服务的安全批量发布工具

在维护窗口里给一组**本地转发服务**更新路由。同一批可能只切成功几台，发布进程也
可能在服务已经切换后丢掉确认。本项目提供：

- 可独立运行、重启后保留生效配置（含多版本规则集与停用项）的**本地转发服务**
  （标准库 HTTP 控制面）；
- 一个**发布工具**：先出满足约束的完整计划，再分批切换，支持查状态、暂停、继续、
  回退、进程崩溃后由新进程接手；
- **固定出口迁移**：把指定流量类别的固定出口从一台现有服务迁到另一台，迁移期间
  每条请求按入站时选定的版本标签逐跳粘性走到对应出口，入口与在途请求都不再依赖
  旧出口后才停用它的接收；
- **故障夹具代理**：复现部分生效、确认丢失、丢请求、抖动；服务内置慢 probe 夹具，
  可让一条老请求真实跨过切换窗口；
- 本地拓扑样例与一条 80 秒内跑完的测试命令（全量实测约 35 秒，迁移演练约 19 秒）。

只依赖 Python 3.11 标准库，无需安装第三方包。所有进程都以 `python3 -I`（隔离模式，
不读当前目录/环境变量，避免被同目录恶意模块劫持）启动。

---

## 1. 目录结构

```
route-release/
├── src/route_release/
│   ├── model.py       规则/拓扑数据模型, 内容指纹, 普通/迁移两套混合状态可达性校验
│   ├── planner.py     满足单批规模与总批数上限的批次规划(回溯+子集枚举, 含迁移/回退顺序)
│   ├── service.py     转发服务: 多版本规则集, 粘性 probe, 停用/恢复, 慢夹具, 原子持久化
│   ├── proxy.py       故障夹具代理(丢确认/丢请求/抖动, 业务错误码原样透传, 可在线切换)
│   ├── state.py       发布进程的持久化状态(原子写, 含排水/停用阶段)
│   ├── publisher.py   发布工具 CLI: submit/submit-migration/run/pause/resume/rollback/status/verify
│   └── launcher.py    一键拉起/关闭整套本地拓扑
├── bin/
│   ├── runmod.py      隔离模式下的模块引导器(-I 时 cwd 不进 sys.path)
│   ├── pub / pub.py   发布工具薄封装
│   └── start_topology.sh
├── examples/          拓扑与发布前/目标配置样例, 含无解与迁移(含坏目标)样例
├── tests/
│   ├── unit_planner.py    纯逻辑单元测试(含迁移规划)
│   ├── e2e_demo.py        普通发布端到端故障演练
│   └── e2e_migration.py   固定出口迁移端到端演练
├── run_demo.sh            普通发布一条命令
├── run_migration_demo.sh  迁移演练一条命令(约 19 秒)
└── run_all_demos.sh       全部测试一条命令(约 35 秒)
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

阶段机：普通发布为 `RUNNING → PAUSED → FORWARD_COMPLETE → ROLLING_BACK →
ROLLED_BACK`；固定出口迁移在正向切完后多出 `FORWARD_COMPLETE → DRAINING →
DECOMMISSIONED`（排水、停用旧出口）。连续不可达时进入 `FAILED`（配置不被破坏，
恢复后 `resume` 即可）。

---

## 3. 快速开始

### 一条命令跑固定出口迁移演练（约 19 秒，要求 80 秒内）

```bash
cd route-release
bash run_migration_demo.sh
```

输出分四部分：目标规则校验（含一个校验不过、保持现网的样例）、部分切换、
跨切换粘性请求与新进程接手停用、按批回退，结尾 `结果: 33 PASS / 0 FAIL`。

### 一条命令跑全部测试（全量约 35 秒，要求 80 秒内）

```bash
cd route-release
bash run_all_demos.sh        # 单元 + 普通发布端到端 + 迁移端到端
# 或只跑普通发布:
bash run_demo.sh
```

输出形如：

```
########## 第一部分: 规划器/模型单元测试(含迁移规划) ##########
  PASS  发布前配置合法
  ...
单元测试全部通过
########## 第二部分: 普通批量发布端到端故障演练(真实本地进程) ##########
场景 0: 无解规划 ...
场景 3: 部分生效——发布进程在批次中途被 kill -9
场景 4: 新进程接手, 先对账再续跑(确认丢失场景随后注入)
...
结果: 22 PASS / 0 FAIL
########## 第三部分: 固定出口迁移端到端(部分切换/跨切换/接手/回退) ##########
...
结果: 33 PASS / 0 FAIL
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

### 4.5 固定出口迁移（把某类流量的出口迁到另一台现有服务）

迁移把流量类别 `video` 的固定出口从 `hub` 迁到 `relay1`。切入口前先出完整计划并
做**目标规则闸校验**（新出口必须已是该类别的 `local_egress`、旧出口必须已改成
转发、不允许第三个节点成为出口）；任一条不过则退出码 2、**不创建状态、不触碰
现网**。

```bash
python3 -I bin/runmod.py route_release.publisher submit-migration \
  --topology ../examples/topology.json \
  --old-rules ../examples/migration_old_rules.json \
  --new-rules ../examples/migration_new_rules.json \
  --registry $WD/registry.json --state $WD/release.json \
  --traffic-class video --old-egress hub --new-egress relay1 \
  --max-batch-size 2 --max-batches 4
python3 -I bin/runmod.py route_release.publisher run --state $WD/release.json
```

关键安全语义：

- **粘性路由**：服务按发布版本标签 `old`/`new` 同时保留两套规则集。请求在**入口**
  按入站时本节点所处版本选定 `pin`，之后逐跳透传，每个节点按该 `pin` 选当前或
  保留版本的规则。于是一条请求从入口到出口始终走同一套规则，迁移窗口内不会出现
  "半新半旧"的错路或环路。
- **分批与闸**：正向沿用单批规模与总批数上限，枚举每批任意部分生效子集（迁移
  类别允许在旧/新出口之一吸收）；上一批全部生效才开下一批。
- **排水与停用**：全部节点切新后进入 `DRAINING`。只有**入口全部切新**且各服务
  上报的旧标签在途计数（入口 `inflight` + 各跳 `visiting`）归零，才调用旧出口
  `/decommission` 停用它对该类别旧版本的接收；服务端还有一道本地闸（现行规则已
  不再吸收 + 在途为 0），双闸任一不过都保持现网。
- **重启接手**：规则集、当前标签、停用项都原子落盘；在途计数是运行态。新进程只凭
  状态文件 + 各服务 `/status` 就能对账续跑排水与停用（见端到端第三部分 kill -9）。
- **回退**：先 `/recommission` 恢复旧出口接收，再重新做受约束规划切回。回退规划
  带顺序约束——旧出口必须在第一批恢复、入口节点不进第一批，避免在旧出口恢复前
  把流量按旧规则送到已不再吸收的节点。回退同样受单批规模与总批数上限约束。

---

## 5. 转发服务接口

每台服务独立监听一个本地端口（注册表 `registry.json`）：

| 方法 | 路径 | 作用 |
| --- | --- | --- |
| GET  | `/status`        | 生效版本、当前标签、保留标签、停用项、在途计数 |
| POST | `/apply`         | 整套切换；可带 `label/previous_label`，切换时按标签保留两套规则集，幂等 |
| POST | `/probe`         | 逐跳真实转发；带 `pin` 时按该版本标签粘性选路，返回完整路径与出口 |
| POST | `/decommission`  | 停用本节点对 `(class,label)` 的接收，前置闸不过返回 409 且不改状态 |
| POST | `/recommission`  | 恢复接收（回退第一步，幂等） |
| POST | `/fixture`       | 慢 probe 夹具：给指定类别注入一次性延迟，让老请求跨过切换窗口 |
| POST | `/shutdown`      | 测试/演示收尾 |

生效规则、按标签保留的规则集与停用项通过"临时文件 + `os.replace`"原子落盘到
`state/<node>.json`，进程重启后自动恢复（见端到端服务重启场景）。在途计数只在
内存，重启即归零（重启本身意味着该节点上的在途连接已结束）。

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

代理对上游的**业务错误码（如 409 粘性请求被停用拦截）原样透传**，只有连接级
故障才包装成 502，这样调用方能读到真实状态码与错误原因。

把某台服务接入代理：启动拓扑时加 `--proxy-node <node>`（可重复）。

服务内置的慢 probe 夹具（`POST /fixture`，仅内存、重启失效）让指定类别的 probe
在该节点延迟一次，端到端演练用它让一条老请求停在中继上、跨过整个切换窗口后再
继续，验证粘性选路与排水闸。

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
- **粘性 pin 为什么用发布级标签 old/new 而不是内容指纹？** 内容指纹是每节点各自
  计算的，入口的指纹在下游节点上并不存在；版本标签是发布全局统一的，入口选定后
  每跳都能按同一标签找到本节点对应的那套规则。
- **为什么停用旧出口要双闸 + 排水？** 只看"入口切新"会漏掉已经在途、仍按旧规则
  走向旧出口的请求。入口在途计数与各跳到达计数都归零、且旧出口现行规则确实不再
  吸收，才允许停用；服务端本地闸再加一道，防止发布进程误判。
- **回退为什么强制旧出口第一批、入口最后？** 旧出口停用后，若入口先切回旧规则，
  流量会被送到一个已经不再吸收该类别的节点。先恢复旧出口接收、最后再切入口，
  才能保证回退的每个中间状态都可达。

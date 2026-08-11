# ch11-classic-backlog-degradation

> **本 module 代码为自研压测演示；博客《Classic 队列为什么一堆积就变慢》原文为纯性能分析，无代码。**

对应博客：[《Classic 队列为什么一堆积就变慢——内存窗口、落盘与流控》](https://www.rabbitmq.com/docs/classic-queues)

本 module 用原生 `amqp-client` 写一个**积压压测 demo**：一个高速 Producer 向**经典队列**（durable）灌大量**持久**消息，一个**慢 Consumer**（`basicQos(1)` + 每条睡眠）制造积压，监控线程实时打印**队列深度、已发布、已消费、发布速率、流控状态、生产者耗时**，直观呈现 Classic 队列「一堆积就变慢」的**断崖**。

---

## 一、原理速览（对应博客）

- **内存窗口只有约 2048 条**（[docs/classic-queues](https://www.rabbitmq.com/docs/classic-queues)：*"Classic queues can store up to 2048 messages in memory, influenced by the consume rate."*），且受消费速率影响。窗口之外的消息全部挤进持久化层落盘——访问介质从 RAM（ns 级）直接降到磁盘（μs~ms 级），吞吐掉一档。
- **越线后四件事叠加成「断崖」**：① 访问主体从 RAM 降到磁盘；② 磁盘访问是随机的，且读写互相争用同一块盘（`queue index` 记位置/投递状态，共享 `message store` 存正文）；③ 内存到 high watermark 时 Broker 直接 **block 生产者（流控 / publisher throttle）**；④ 内存抖动 / 换页 / index 膨胀。所以是「陡降」而非「慢慢变慢」。
- **3.12 的两项优化（[docs/persistence-conf](https://www.rabbitmq.com/docs/persistence-conf)）**：① **大于 4096 字节的消息不再提前读回内存**，等真要投递时才从磁盘现取现发，避免几个大消息吃光内存预算；② **给每个队列的磁盘缓存封顶约 1MB**（`classic_queue_store_v2_max_cache_size`），按队列算、可预期、不会失控。两者把「内存里到底放多少」从「可能突然暴涨」变成「有上限、可预期」，削平了断崖——这是 v2 在高内存压力下更稳的具体落点。
- **lazy-mode 已在 3.12 移除，行为并入 Classic 默认实现（[docs/lazy-queues](https://www.rabbitmq.com/docs/lazy-queues)）**：*"RabbitMQ no longer supports the 'lazy' mode for classic queues."* 注意 **lazy 不是「被 Quorum 替代」**——它的能力被并进了 Classic 默认，这个开关本身退役了。
- **Quorum 与 Stream 各管一摊**：**Quorum**（3.8）管**可靠性 / 一致性**（Raft 复制、过半确认、durable 默认），并不擅长海量积压；真正把「堆积 → 断崖」从根上解决的是 **Stream**（3.9，append-only 日志、按 offset 回放、百万级积压仍低内存高吞吐），**Stream 才管大积压吞吐**。

---

## 二、运行方式

前置：本机（或可达的）RabbitMQ 3.10+（推荐 3.12/3.13），开启管理台插件。连接参数默认 `localhost:5672`、`admin/admin`、`vhost=/`，可在 `application.yml` 改。

```bash
# 在仓库根目录
mvn -pl ch11-classic-backlog-degradation -am spring-boot:run
```

或在 IDE 里直接运行 `io.github.codecorey.backlog.BacklogDemoApplication`。

启动后流程：
1. 清空队列 `backlog.classic`（`purge-on-start=true`）；
2. 启动慢消费者（`basicQos(1)` + 每条 50ms）；
3. 启动监控守护线程（默认每 2s 打印一次）；
4. 高速 Producer 全速灌 50000 条持久化消息；
5. 灌完后 `spring.main.keep-alive` 保活，慢消费者继续排空，监控打印队列深度下降，深度归零后监控自行退出（进程仍保活，Ctrl+C 退出）。

---

## 三、配置项（`application.yml` 的 `app.*`）

| 配置项 | 默认值 | 说明 |
|------|------|------|
| `app.host` / `app.port` | `localhost` / `5672` | Broker 地址端口 |
| `app.username` / `app.password` | `admin` / `admin` | 登录账号 |
| `app.virtual-host` | `/` | vhost |
| `app.queue` | `backlog.classic` | 经典队列名（durable=true） |
| `app.message-count` | `50000` | 灌入消息总数 |
| `app.consumer-sleep-ms` | `50` | 慢消费者每条睡眠 ms（50ms → 约 20 条/s） |
| `app.payload-bytes` | `256` | 单条消息负载字节数（≤4096 走 message store 内嵌） |
| `app.monitor-interval-ms` | `2000` | 进度 / 深度 / 发布速率打印间隔 |
| `app.purge-on-start` | `true` | 启动时清空队列，保证可复现 |

想跑快点：调小 `message-count`；想让流控更容易触发：调大 `payload-bytes`、调大 `message-count`，或把 Broker 的 `vm_memory_high_watermark` 调低（见下）。

---

## 四、怎么观察「断崖」

### 1. 看日志（控制台）

监控线程会持续打印类似：

```
[监] 已发布=18234, 已消费=146, 队列深度=18088, 近2000ms发布速率≈9117条/s, 流控=正常, 生产者耗时=-1ms
[!] Broker 触发流控（BLOCK 生产者）：memory_high_watermark —— 发布速率将出现断崖
[监] 已发布=19890, 已消费=148, 队列深度=19742, 近2000ms发布速率≈828条/s, 流控=BLOCKED, 生产者耗时=-1ms
```

能看到两件事：
- **队列深度持续增长**（Producer 远快于 Consumer，迅速顶破约 2048 条内存窗口）；
- **发布速率从 RAM 速度陡降**到磁盘 bound，甚至出现 `BLOCKED`（流控）——这就是「断崖」。

### 2. 看管理台（推荐）

打开 RabbitMQ 管理台 → Queues → 选 `backlog.classic`，看它的图表：
- **Messages（深度）曲线**：一路爬升，Producer 发完后转为缓慢下降（Consumer 以 ~20 条/s 排空）；
- **Publish 速率曲线**：高位运行，越过内存窗口/触发流控后**明显下台阶**；
- **Deliver (ack) 速率曲线**：始终被 `basicQos(1)` + 50ms 压在低位，与 Publish 形成巨大落差，正是积压的来源。

把 Publish 速率和队列深度两条曲线叠着看，「断崖」一目了然。

### 3. 如何更容易触发流控（BLOCKED）

默认 Broker 的 `vm_memory_high_watermark = 0.4`（可用内存的 40%），开发机内存大时不易触发。想稳定看到 `connection.blocked`，可在 Broker 侧把水位调低，例如 Docker 启动时：

```bash
docker run -d --hostname rmq --name rmq \
  -p 5672:5672 -p 15672:15672 \
  -e RABBITMQ_VM_MEMORY_HIGH_WATERMARK='{"absolute":256MB}' \
  rabbitmq:3.13-management
```

把绝对水位压到 256MB 左右，50000 条 × 较大 payload 很容易顶破，Producer 日志立刻打印 `BLOCKED`，管理台该连接也会显示 `blocked`。

---

## 五、代码结构（均为自研）

```
io.github.codecorey.backlog
├── BacklogDemoApplication     # @SpringBootApplication 入口
├── config
│   └── BacklogProperties       # @ConfigurationProperties("app")：连接参数 + 压测参数
├── consumer
│   └── SlowConsumer            # basicQos(1) + 每条睡眠，制造积压；AtomicLong 计已消费
├── producer
│   └── HighSpeedProducer       # 全速发 PERSISTENT_TEXT_PLAIN；BlockedListener 监听流控
├── monitor
│   └── QueueMonitor            # queueDeclarePassive 周期采样深度，打印进度/速率/流控
├── runner
│   └── BacklogRunner           # CommandLineRunner：清空→启动消费者→监控→灌消息
└── support
    └── Connections             # 按配置构建 ConnectionFactory 的公共工具
```

- 三条独立连接：消费者、生产者、监控各用一条，互不阻塞，且 Channel 非线程安全也互不冲突。
- 队列深度用 `channel.queueDeclarePassive(queue).getMessageCount()` 读取（被动声明：只回读，不创建/不改属性）。
- 生产者通过 `connection.addBlockedListener(...)` 捕获 Broker 流控的 `connection.blocked` / `unblocked` 事件。
- `spring.main.keep-alive: true`（非 Web 应用）：Producer 发完后进程不退出，慢消费者继续排空，便于观察队列深度下降。

---

## 六、官方文档

- Classic 队列（含 ~2048 条内存窗口）：https://www.rabbitmq.com/docs/classic-queues
- 持久化层 / v1 vs v2 / 3.12 优化：https://www.rabbitmq.com/docs/persistence-conf
- lazy-mode 已在 3.12 移除：https://www.rabbitmq.com/docs/lazy-queues

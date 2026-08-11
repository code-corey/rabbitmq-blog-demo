# ch13-amqp-and-protocols

> **本 module 只有一个 AMQP 0-9-1 基线 demo；MQTT / STOMP / Stream 协议的讲解与测试在 README 里完成（用命令行客户端，不写 Java demo）。**

对应博客：[《AMQP 1.0 与多协议——MQTT、STOMP、Stream》](/中间件/rabbitmq/rabbitmq-13-amqp-and-protocols)

RabbitMQ 区别于纯 AMQP broker 的核心卖点之一，是**一个集群同时讲多种协议**：物联网设备用 MQTT 推数据，后端 Java 服务用 AMQP 消费，浏览器前端用 STOMP over WebSocket 订阅通知——在 RabbitMQ 里是同一件事。本篇就来理清这几套协议的区别、选型与互操作。

---

## 一、为什么只写 AMQP 0-9-1 demo（依赖取舍）

本 module 的重点是**讲清多协议并存与互操作**，而不是把每种协议都跑一遍 Java 代码。原因：

- **MQTT** Java 侧需要 Eclipse Paho（`org.eclipse.paho:org.eclipse.paho.client.mqttv3`）等额外客户端库；**STOMP** 需要 FuseSource `stomp-client` 或 `spring-messaging`；每加一种协议就多一套连接模型、多一条重依赖。
- `com.rabbitmq:amqp-client` **只讲 AMQP 0-9-1**（RabbitMQ 母语，也是本系列前 12 篇用的协议）。一条 0-9-1 收发基线已足够代表「默认协议」怎么连 Broker。
- MQTT / STOMP 是**文本/轻量协议**，用命令行客户端（`mosquitto_pub` / `nc`）验证更轻、更直观，还能直接演示**跨协议互操作**（MQTT 发、AMQP 收）。

所以：**Java demo 做 AMQP 0-9-1 基线，其余协议用命令行测。** 保持构建简单。

---

## 二、RabbitMQ 支持的协议一览

| 协议 | 插件 | 默认端口（明文 / TLS） | 典型场景 | 4.x 状态 |
|------|------|:---:|------|------|
| **AMQP 0-9-1** | 内置 | 5672 / 5671 | 后端服务间通信（本系列默认） | 原生 |
| **AMQP 1.0** | 内置 | 5672 / 5671 | 跨 broker 互通、新项目 | 4.0 起原生 |
| **MQTT** | `rabbitmq_mqtt` | 1883 / 8883 | 物联网、海量设备、低带宽 | 内置插件 |
| **STOMP** | `rabbitmq_stomp` | 61613 / 61614 | 简单文本、浏览器 / 多语言 | 内置插件 |
| **Stream 协议** | `rabbitmq_stream` | 5552 / 5551 | 高吞吐日志、Stream 原生访问 | 内置插件 |
| 管理 HTTP API | `rabbitmq_management` | 15672 / 15671 | 运维、监控 | 内置插件 |

> **AMQP 不需要启用插件**（0-9-1 和 1.0 都开箱即用）；MQTT、STOMP、Stream 需要 `rabbitmq-plugins enable`。所有插件都随发行版附带，不用额外下载。
>
> **同一个 5672 怎么同时承载 0-9-1 和 1.0？** 客户端建好 TCP/TLS 连接后、发任何 AMQP 帧之前，先发一个**协议头（protocol header）**声明版本，Broker 据此走对应协议栈——叫 **Version Negotiation**。

---

## 三、各协议要点

### 3.1 AMQP 0-9-1（母语）

RabbitMQ 内部的 exchange / queue / binding 模型就是按 0-9-1 设计的。客户端生态最全（Java、Python、Go、PHP、Ruby…），稳定可靠。`com.rabbitmq:amqp-client` 讲的就是它。**本 module 的 demo 即 0-9-1 基线。**

### 3.2 AMQP 1.0（4.0 起核心协议）

**AMQP 1.0 不是 0-9-1 的补丁，而是几乎重新设计的协议**（ISO/IEC 19464、OASIS 标准；0-9-1 反而从未成为正式标准）。4.0 起 RabbitMQ 原生支持，无需插件。它带来：细粒度流控（单连接上某条队列堵了不影响其他队列）、服务端过滤表达式、队列本地性、Modified 结局、Stream 存储零损耗等。

- **寻址**：RabbitMQ 自定义 v2 地址格式——`/queues/:queue`、`/exchanges/:exchange/:routing-key`，名字要 percent-encoding。
- **客户端**：官方 1.0 客户端只有 **Java** 和 **.NET** 两款（`rabbitmq-amqp-java-client`），选前先确认你的语言有没有趁手的 1.0 客户端。
- **一句话**：新项目、客户端语言允许，优先 1.0（官方未来方向）；老项目或生态受限，0-9-1 仍稳妥。

### 3.3 MQTT（物联网）

IoT 事实标准：轻量、低带宽、支持不稳定网络。RabbitMQ 通过 `rabbitmq_mqtt` 支持 MQTT 3.1 / 3.1.1 / 5.0。

```bash
rabbitmq-plugins enable rabbitmq_mqtt      # 默认监听 1883（明文）/ 8883（TLS）
```

- **Topic** 用 `/` 分层，支持 `+`（单层）和 `#`（多层）通配符；**QoS 0/1** 支持，**QoS 2 不支持**（3.1.1 客户端降级到 QoS 1，5.0 客户端断开）。
- 特色：**Retained**（新订阅者一上线收到最后一条保留消息）、**Will Message**（异常断线时代发的「遗嘱」）、**Clean Session**。
- **落地映射**：MQTT topic 消息统一路由到一个 topic exchange（默认 `amq.topic`）；每个订阅者一个专属队列 `mqtt-subscription-<客户端ID>qos[0|1]`。
- **分隔符翻译坑**：MQTT 的 `/` ↔ AMQP 的 `.`，`+` ↔ `*`。所以 **MQTT topic 里别带点号 `.`**，**AMQP routing key 里别带斜杠 `/`**。

### 3.4 STOMP（纯文本）

纯文本协议，帧就是一行行可读文本，命令只有 `CONNECT` / `SEND` / `SUBSCRIBE` / `ACK` / `NACK` 等几个，调试时甚至能直接用 telnet 手敲。

```bash
rabbitmq-plugins enable rabbitmq_stomp     # 默认监听 61613（明文）/ 61614（TLS）
```

用 `destination` 头寻址（没有 exchange/queue 概念）：

| 前缀 | 含义 |
|------|------|
| `/topic/<name>` | 发布订阅到 `amq.topic`（无订阅者即丢弃） |
| `/queue/<name>` | 共享队列（首次 SEND 时自动创建） |
| `/amq/queue/<name>` | 操作已存在的队列（不自动创建） |
| `/exchange/<name>[/<key>]` | 发到任意 exchange / 用任意 binding 订阅 |
| `/temp-queue/<x>` | 临时队列（只能用在 `reply-to` 头里） |

> 单帧默认上限 **4 MB**（`stomp.max_frame_size`）。浏览器前端用 STOMP over WebSocket 配 `rabbitmq_web_stomp`。

### 3.5 Stream 协议（原生二进制高吞吐）

注意区分两个概念：**Stream 队列类型**（append-only 日志，任意协议可消费，见[第 07 篇](/中间件/rabbitmq/rabbitmq-07-queue-types)）vs **Stream 协议**（`rabbitmq_stream` 提供的原生二进制协议，吞吐最高的访问方式）。

```bash
rabbitmq-plugins enable rabbitmq_stream            # 默认监听 5552（明文）/ 5551（TLS）
rabbitmq-plugins enable rabbitmq_stream_management  # 管理台 Stream 监控页（可选）
```

- 消息按 **chunk**（消息批次）存与传，一个信用 = 一个 chunk；流控有 publisher 信用流 + consumer 信用流两层。
- 支持拓扑发现：发布连 leader、消费连副本，减少集群内转发。
- 客户端：Java、Go、.NET、Rust、Python（rstream）。适合日志、事件溯源、大数据管道等极限吞吐场景。

---

## 四、启用插件

```bash
# AMQP 0-9-1 / 1.0 无需插件，开箱即用

rabbitmq-plugins enable rabbitmq_mqtt               # MQTT：1883 / 8883
rabbitmq-plugins enable rabbitmq_stomp              # STOMP：61613 / 61614
rabbitmq-plugins enable rabbitmq_web_stomp          # STOMP over WebSocket（浏览器）
rabbitmq-plugins enable rabbitmq_stream             # Stream 协议：5552 / 5551
rabbitmq-plugins enable rabbitmq_stream_management  # Stream 管理台页（可选）
rabbitmq-plugins enable rabbitmq_management         # 管理 HTTP API：15672
```

> 默认账号 `guest / guest`（仅 localhost）；本系列 Docker 镜像建了 `admin / admin`。MQTT/STOMP/Stream 登录用同样的账号。

---

## 五、怎么选协议

| 你的场景 | 推荐协议 | 理由 |
|------|------|------|
| 后端 Java / .NET 服务间通信 | **AMQP 1.0** | 官方未来方向，能力最强；客户端成熟 |
| 后端多语言服务、存量系统 | **AMQP 0-9-1** | 客户端生态最丰富，稳定可靠 |
| 物联网、海量设备、低带宽 | **MQTT** | IoT 事实标准，轻量、Will / Retained |
| 浏览器前端实时通知 | **STOMP over WebSocket** | 文本协议，JS 原生友好 |
| 极致吞吐、日志 / 事件管道 | **Stream 协议** | 原生二进制，吞吐最高 |
| 快速原型、脚本调试 | **STOMP** | 文本帧，telnet 都能玩 |
| 跨 broker 迁移 / 互通 | **AMQP 1.0** | 行业标准，多家 broker 支持 |

**一句话**：默认 AMQP，IoT 选 MQTT，求简用 STOMP，拼吞吐上 Stream 协议。

---

## 六、跨协议互操作（核心价值 + 坑）

多协议的真正价值是**同一集群里不同协议客户端能互相收发**。因为内核是 AMQP 0-9-1 模型，所有协议底层都映射到 exchange / queue / binding：

| 协议 | 发布映射到 | 消费映射到 |
|------|------|------|
| AMQP 0-9-1 | exchange（原生） | queue（原生） |
| AMQP 1.0 | exchange（v2 地址） | queue（v2 地址） |
| MQTT | topic exchange（默认 `amq.topic`） | 每订阅者一个专属队列 |
| STOMP | 按 destination 前缀映射 | 按 destination 前缀映射 |
| Stream 协议 | 直接写 Stream 日志 | 直接读 Stream 日志 |

**常见组合与坑：**

| 组合 | 能否互通 | 注意点 |
|------|:---:|------|
| MQTT 发 → AMQP 收 | ✅ | AMQP 队列绑定到 `amq.topic`，binding key 把 MQTT 的 `/` 换成 `.` |
| AMQP 发 → MQTT 收 | ✅ | AMQP 发到 `amq.topic`，routing key 的 `.` 换成 `/` 当 MQTT topic |
| STOMP 发 → AMQP 收 | ✅ | STOMP 用 `/exchange` 或 `/amq/queue` 目的地 |
| 任意协议 → Stream 消费 | ⚠️ | AMQP / STOMP 可消费 Stream（设 `x-stream-offset`），但 Stream 协议吞吐最佳 |
| MQTT → Stream | ⚠️ | MQTT 可向 Stream 发（Stream 绑到 topic exchange），但**不能直接从 Stream 消费** |
| QoS 2 跨协议 | ❌ | RabbitMQ 根本不支持 QoS 2 |

> **分隔符翻译是最大的坑**：MQTT topic `cities/london` 会变成 AMQP routing key `cities.london`——跨协议消费时对不上就是这里没翻译。

---

## 七、用命令行客户端测 MQTT / STOMP（无需 Java）

### 7.1 MQTT：`mosquitto_pub` / `mosquitto_sub`

先在 Broker 侧启用插件：`rabbitmq-plugins enable rabbitmq_mqtt`

```bash
# 订阅（另开一个终端，会阻塞等待）
mosquitto_sub -h localhost -p 1883 -t "cities/london/weather" -v

# 发布一条 QoS 1 消息（用 admin 账号；guest 仅 localhost）
mosquitto_pub  -h localhost -p 1883 -u admin -P admin \
               -t "cities/london/weather" -m '{"temp":18.5}' -q 1

# 通配订阅：cities/ 下所有
mosquitto_sub -h localhost -p 1883 -t "cities/#" -v
```

> **跨协议验证**：MQTT 发到 `cities/london/weather`，会被路由到 `amq.topic`、routing key = `cities.london.weather`。用本系列的 AMQP 消费者把队列绑定到 `amq.topic` 且 binding key = `cities.london.weather`（或 `cities.#`）就能收到——这就是「MQTT 发、AMQP 收」。

### 7.2 STOMP：`nc`（netcat）手敲文本帧

先启用插件：`rabbitmq-plugins enable rabbitmq_stomp`

STOMP 是纯文本，用 `nc` 连上 61613 就能直接发帧（每帧以一个 NUL 字符 `\x00` 结束）：

```bash
# CONNECT + SEND 一气呵成（printf 注入 ^@ NUL 终止符；consumed by AMQP 消费者从 queue orders 取）
{
  printf 'CONNECT\naccept-version:1.2\nhost:/\nlogin:admin\npasscode:admin\n\n\x00'
  sleep 1
  printf 'SEND\ndestination:/queue/orders\ncontent-type:text/plain\n\nhello from STOMP\x00'
} | nc localhost 61613
```

> `/queue/orders` 会自动创建（或复用）名为 `orders` 的队列。发完后用本系列任一 AMQP 消费者从队列 `orders` 拉取，即可看到 `hello from STOMP`——「STOMP 发、AMQP 收」。
>
> 浏览器场景装 `rabbitmq_web_stomp`，前端用 `stompjs` 走 WebSocket 即可，无需写 Java。

### 7.3 Stream 协议

Stream 协议是二进制、有自己的客户端库，命令行不便手敲。快速体验推荐官方 `rabbitmq-stream-perf-test` 工具（Java），或 Python `rstream`。详见 [docs/streams](https://www.rabbitmq.com/docs/streams)。

---

## 八、本 module 的 demo 与运行方式

`AmqpBaselineDemo` 做的事（对应博客一、二节「默认协议」）：

1. 用原生 `amqp-client` 连本机 Broker（5672，AMQP 0-9-1）；
2. 打印**协议信息**：本 demo 用 AMQP 0-9-1、端口 5672（与 1.0 共用、靠协议头协商）、Broker 的 product/version/cluster、客户端版本；
3. 声明一个**临时 exclusive 队列**，发一条持久化消息，再 `basicGet` 拉回、手动 ACK，完成一个来回；
4. 关闭上下文、进程退出。

前置：本机（或可达的）RabbitMQ，连接参数默认 `localhost:5672`、`admin/admin`、`vhost=/`，可在 `src/main/resources/application.yml` 改。

> **本 module 尚未注册到根 pom 的 `<modules>`（按要求不改动根 pom）**，在 module 目录内独立运行：

```bash
cd ch13-amqp-and-protocols
mvn spring-boot:run
```

或从仓库根：`mvn -f ch13-amqp-and-protocols/pom.xml spring-boot:run`

IDE 里直接运行 `io.github.codecorey.protocols.ProtocolsApplication`。

预期日志：

```
====== 协议信息 ======
本 demo 协议: AMQP 0-9-1（com.rabbitmq:amqp-client 默认且唯一支持的协议，RabbitMQ 母语）
AMQP 端口: 5672（0-9-1 与 1.0 共用，靠连接初始的协议头协商版本）
Broker: RabbitMQ 3.13.x（cluster=rabbit@..., platform=...）
Client: AMQP-client for Java 5.21.0
====== AMQP 0-9-1 收发基线 ======
声明临时队列: amq.gen-xxxx
[x] 已发送: hello from amqp 0-9-1 @ ...
[o] 已收到: hello from amqp 0-9-1 @ ...
```

---

## 九、代码结构

```
io.github.codecorey.protocols
├── ProtocolsApplication        # @SpringBootApplication 入口
├── config
│   └── ProtocolProperties       # @ConfigurationProperties("rabbitmq")：连接参数
└── demo
    └── AmqpBaselineDemo         # CommandLineRunner：连 Broker、打印协议信息、收发一条
```

---

## 十、官方文档

- AMQP 1.0（4.0 起核心）：https://www.rabbitmq.com/docs/amqp
- MQTT 插件：https://www.rabbitmq.com/docs/mqtt
- STOMP 插件：https://www.rabbitmq.com/docs/stomp
- Stream 协议：https://www.rabbitmq.com/docs/streams
- 插件管理：https://www.rabbitmq.com/docs/plugins

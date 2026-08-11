# ch08-sharding —— 消息分片存储插件 Sharding

博客《RabbitMQ 系列 · 第 8 篇：消息分片存储插件 Sharding》配套示例。

用 **原生 amqp-client** 演示 RabbitMQ **Sharding 插件**：把单个逻辑交换机上的消息轮询分散到多个分片
Queue，Consumer 通过 **伪队列（pseudo queue）** 统一消费，在 Consumer 扩容困难时用空间换吞吐。

> 思路类似数据库分库分表：分库减 IO 压力，分表解决单表过大；Sharding 针对 **单队列吞吐** 做水平拆分。
> 适用：**对延迟要求不严格、对顺序无要求** 的场景（分片过程不考虑消息顺序）。

---

## ⚠️ 运行前置（必须先完成）

代码本身能编译，但 **实跑依赖 Broker 端的插件与策略**。未做前置直接运行会在 `exchangeDeclare`
时报错（`unknown exchange type 'x-modulus-hash'`）。

### 1. 启用 Sharding 插件

3.13 运行包已内置，直接启用：

```bash
rabbitmq-plugins enable rabbitmq_sharding
```

启用后，Exchange 类型会多出一种 **`x-modulus-hash`**。

### 2. 配置 Sharding 策略（Admin → Policies）

Sharding 通过 **策略（policy）** 驱动，必须匹配到 `x-modulus-hash` 交换机上才会自动创建分片 Queue。
博客以截图展示，下面是等价的文字步骤。

**方式 A：管理控制台 UI**

打开 RabbitMQ 管理界面 → **Admin** → **Policies** → **Add / update a policy**，填写：

| 字段 | 值 | 说明 |
|------|-----|------|
| Virtual host | `/`（或你使用的 vhost） | 策略所在 vhost |
| Name | `sharding-policy` | 策略名，自定义 |
| Pattern | `^sharding_` | 正则，匹配以 `sharding_` 开头的交换机 |
| Apply to | `Exchanges` | **必须选 Exchanges**（策略作用于交换机） |
| Priority | `0` | 优先级，默认即可 |
| Definition | key=`sharding-definition`，value=`{"shards-per-node":3}` | 见下方说明 |

Definition 添加一行：
- **key**：`sharding-definition`
- **value**：`{"shards-per-node":3}` —— 表示每个节点 3 个分片。

**方式 B：rabbitmqctl 命令行**

```bash
# 对默认 vhost / 下、匹配 ^sharding_ 的交换机应用 sharding 策略，每节点 3 个分片
rabbitmqctl set_policy --apply-to exchanges sharding-policy "^sharding_" \
  '{"sharding-definition":{"shards-per-node":3}}'
```

> **关于分片数**：总分片 Queue 数 = `shards-per-node` × 节点数。
> - 单节点 Broker + `shards-per-node=3` → 共 3 个分片 Queue。
> - 3 节点集群 + `shards-per-node=1` → 共 3 个分片 Queue（每节点 1 个）。
>
> 本模块消费端 `app.shards`（默认 3）应与 **总分片数** 保持一致，才能均匀消费。
> Broker 端 `shards-per-node` 与本模块 `app.shards` 是两处独立配置，请按你的集群规模对齐。

### 3. 关键机制（影响代码理解）

- **`x-modulus-hash` 忽略 routingKey 的语义**：以轮询方式平均分配到绑定的所有分片 Queue，routingKey
  只用作普通字符串占位（本例传序号 `String.valueOf(i)`）。
- **伪队列物理不存在**：消费端声明的「队列」（名字 = 交换机名）并不是真正的 Queue，插件在内部把
  `basicConsume` 请求路由到 **当前连接数最少** 的分片 Queue。
- **必须 ACK**：Consumer 使用手动 ACK（`autoAck=false` + `basicAck`），未确认的消息会被持续重投。

---

## 运行方式

默认连接 `localhost:5672`、用户 `admin/admin`、vhost `/`，可在 `src/main/resources/application.yml`
中修改（博客原文硬编码 `192.168.65.112` + vhost `/mirror`）。

### 1. 生产者：向 `x-modulus-hash` 交换机发 3000 条消息

`application.yml` 默认 `app.mode=send`，直接运行即发送，发完进程退出：

```bash
mvn -pl ch08-sharding spring-boot:run
```

或显式指定参数：

```bash
mvn -pl ch08-sharding spring-boot:run -Dspring-boot.run.arguments="--app.mode=send --rabbitmq.practice.message-count=3000"
```

发送后到管理界面可看到分片 Queue，名字形如 `sharding:sharding_exchange-<node>-<index>`，消息大致均分。

### 2. 消费者：用伪队列消费（按分片数 `basicConsume` N 次）

切换为 consumer 模式（默认缺省即 consumer，会与 send 模式互斥；显式覆盖）：

```bash
mvn -pl ch08-sharding spring-boot:run -Dspring-boot.run.arguments="--app.mode=consumer --app.shards=3"
```

或直接注释掉 `application.yml` 里的 `mode: send`，让 `matchIfMissing` 走默认 consumer。

消费者对 **同一个伪队列**（名字 = 交换机名）调用 `app.shards` 次 `basicConsume`，分散消费各分片。
非 Web 应用，靠 `spring.main.keep-alive: true` 保活，持续消费，Ctrl+C 退出。

> 典型演示顺序：**先 `--app.mode=consumer` 启动消费者**（阻塞等待），**再 `--app.mode=send` 发消息**，
> 即可看到消息被多个分片消费者分散处理。

---

## 配置项

| 配置键 | 默认值 | 说明 |
|--------|--------|------|
| `rabbitmq.practice.host` | `localhost` | Broker 地址 |
| `rabbitmq.practice.port` | `5672` | Broker 端口 |
| `rabbitmq.practice.username` | `admin` | 用户名 |
| `rabbitmq.practice.password` | `admin` | 密码 |
| `rabbitmq.practice.virtual-host` | `/` | vhost |
| `rabbitmq.practice.exchange` | `sharding_exchange` | 交换机名（同时是伪队列名，需匹配策略 Pattern） |
| `rabbitmq.practice.message-count` | `3000` | 生产者发送的消息条数 |
| `app.mode` | `send` | `send`=生产者 / `consumer`=消费者（缺省=consumer） |
| `app.shards` | `3` | 对伪队列 `basicConsume` 的次数，应对齐 Broker 端总分片数 |

> 交换机名必须以 `sharding_` 开头才能命中本例策略 Pattern `^sharding_`。如改 `exchange`，请同步调整策略。

---

## 代码结构

| 类 | 对应博客 | 作用 |
|----|---------|------|
| `ShardingApplication` | — | `@SpringBootApplication` 入口 |
| `config.RabbitMqProperties` | — | `@ConfigurationProperties` 绑定连接与交换机参数 |
| `producer.ShardingProducer` | `ShardingProducer` | 声明 `x-modulus-hash` 交换机并 `basicPublish` N 条 |
| `consumer.ShardingConsumer` | `ShardingConsumer` | 伪队列 + `DefaultConsumer`，按 `app.shards` 调用 N 次 `basicConsume`，手动 ACK |

---

## 注意事项（来自博客）

| 注意点 | 说明 |
|--------|------|
| **顺序** | 分片不考虑顺序，不适合强顺序业务 |
| **均匀性** | 轮询尽量均匀，但不保证绝对均匀 |
| **伪队列与分片 Queue 勿混用** | 分片 Queue 若已有大量其他消息，再消费伪队列会受不均匀数据影响 |
| **Producer 视角** | 只发虚拟 Exchange，无法预知具体分片 |
| **Ack** | 未 Ack 的消息会持续重投，须正常 `basicAck` |

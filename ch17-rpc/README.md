# ch17-rpc —— RPC 模式：用 RabbitMQ 实现远程调用

> 对应博客《[RPC 模式——用 RabbitMQ 实现远程调用](https://langkemaoxin.github.io/)》（RabbitMQ 系列 第 17/22 篇）
>
> 原生 `amqp-client`（AMQP 0-9-1）演示：RPC 服务端消费 `rpc.queue`，客户端用 **direct reply-to** 发请求并阻塞等应答。

---

## 一、RPC 是什么：请求 / 应答的"伪同步"

RPC（Remote Procedure Call）= 调用方（client）发一条请求消息，**阻塞等着**收一条应答消息，拿到结果才继续。对调用方而言，看起来就像调了个本地方法。

和 [05 消息场景](../ch05-messaging-patterns) 里那六种单向（one-way）模式最大的区别：

| 维度 | 单向模式（Work / Pub-Sub / Routing / Topic） | RPC 模式 |
|------|----------------------------------------------|----------|
| 方向 | 单向：Producer → Queue → Consumer | 双向：请求 → 处理 → **应答原路返回** |
| 调用方是否等待 | 发完即走 | **阻塞等应答** |
| 需要几个队列 | 一个请求队列 | 请求队列 **+ 每个调用方一个回复队列** |
| 消息关联 | 不关心谁发的 | 必须用 **`correlationId`** 把应答和请求对上 |

一句话：单向模式是"喊一嗓子就走"，RPC 是"喊一嗓子、还非得等到对方回话"。

### 两条命脉属性

| 属性 | 谁设置 | 干什么用 |
|------|--------|----------|
| **`replyTo`** | 请求方（client） | 告诉服务端："处理完把结果发到这个地址" |
| **`correlationId`** | 请求方生成、服务端原样回填 | 应答消息的唯一"回执号"，client 靠它把应答匹配到对应的请求 |

---

## 二、经典 reply 队列 vs direct reply-to

### 经典实现（官方 tutorial 6）

client 声明一个**临时回复队列**（exclusive、autodelete，断开即删），每次请求复用同一个队列，靠 `correlationId` 区分多条应答。

固有开销：建/删队列要写元数据存储、集群全节点共识、回复消息进队列缓冲、还要为它起独立 Erlang 进程。客户端一多，开销就堆起来。

### direct reply-to（本模块采用）

RabbitMQ 专属优化：**干脆不要回复队列**。client 直接消费固定伪队列名 `amq.rabbitmq.reply-to`，Broker 内部根本没有这个队列实体。

| 步骤 | 谁 | 做什么 |
|------|-----|--------|
| 1 | client | no-ack 模式下 `basicConsume("amq.rabbitmq.reply-to", true, ...)` |
| 2 | client | 发布请求时 `replyTo` 也设成 `amq.rabbitmq.reply-to` |
| 3 | Broker | 透明改写 `replyTo` 成 `amq.rabbitmq.reply-to.<不透明后缀>`（每连接唯一） |
| 4 | server | 处理完，往默认交换机 `""` 发布应答，路由键即改写后的名字 |
| 5 | Broker | 直接把应答送到 client 的连接/通道进程，**不经过任何队列缓冲** |

| 指标 | 经典 reply 队列 | direct reply-to |
|------|----------------|-----------------|
| 元数据存储（建/删队列） | 有 | **无** |
| 回复消息缓冲 | 有 | **无（零缓冲）** |
| 独立 Erlang 进程 | 有 | **无** |
| 管理界面 / `list_queues` 能看到 | 能 | **不能** |
| 投递语义 | 至少一次 | **至多一次**（client 断了就丢） |

> "直接"（direct）这个词容易误解：它**仍然经过 Broker**，client 和 server 之间没有点对点的网络直连。区别只在 Broker 内部省掉了队列这一层。

---

## 三、correlationId 如何匹配请求与应答

一个 client 复用一个回复通道时，会**并发发多个请求**，回复通道里陆续收到多条应答。到底哪条对应哪次请求？就靠 `correlationId` 一一对应。

本模块 `RpcClient` 的做法（见 `RpcClient.java`）：

1. 每次调用生成 `UUID.randomUUID()` 作 `correlationId`（全局唯一，别用自增 ID，多实例会撞）；
2. 维护 `ConcurrentHashMap<correlationId, BlockingQueue>`（并发安全，回复消费者与业务线程并发读写）—— 每个 `correlationId` 对应一个单槽等待队列；
3. 回复消费者收到应答按 `correlationId` 取出对应等待队列，`offer` 应答体；
4. 调用方 `BlockingQueue.poll(timeout)` 阻塞拿结果；拿到后**及时移除映射项**，防内存泄漏。

---

## 四、模块代码结构

```
src/main/java/io/github/codecorey/rpc
├── RpcApplication.java        # @SpringBootApplication 入口
├── RpcServer.java             # 消费 rpc.queue，处理 fib:N / upper:xxx，应答发回 replyTo
├── RpcClient.java             # direct reply-to 客户端：correlationId + ConcurrentHashMap 等应答
├── RpcDemoRunner.java         # CommandLineRunner：先起 server，再用 client 发 5 条 RPC
└── config
    ├── RabbitMqProperties.java  # 绑定 rabbitmq.practice.* 连接配置
    └── RabbitMqConfig.java      # 共享 amqp-client ConnectionFactory
```

### 请求协议（演示用前缀协议）

| 请求体 | 含义 | 期望应答 |
|--------|------|----------|
| `fib:10` | 算 fib(10) | `55` |
| `fib:20` | 算 fib(20) | `6765` |
| `fib:0` | 算 fib(0) | `0` |
| `upper:hello rpc` | 转大写 | `HELLO RPC` |

服务端不识别的格式会回 `ERROR: ...`（业务异常也回应答，不让 client 干等到超时）。

### direct reply-to 的硬规矩（`RpcClient.java` 注释里也写了）

- 必须 **no-ack** 模式消费 `amq.rabbitmq.reply-to`；
- 发布请求和消费应答必须用**同一个连接、同一个通道**；
- 每条通道**最多一个** direct reply-to 消费者；
- 应答**不是容错的**：client 一断开，路上的应答被 Broker 直接丢弃，重连后需自行重发；
- `amq.rabbitmq.reply-to` **不是真队列**——不可删、不在管理界面出现、`rabbitmqctl list_queues` 也看不到；
- server 发布时带 `mandatory` 标志时，这个伪名字会被当作"已路由"，**不会**触发 `basic.return`。

---

## 五、何时该用 / 不该用 RPC over MQ

> 经验法则：如果整个系统最核心的调用都是 RPC over MQ，那架构大概率出问题了。它该是少数派，是"顺手补一刀"的用法，不是主调用通道。

### 适合

- 海量短请求/应答（数万 client）——direct reply-to 省队列优势最明显；
- 高连接抖动（来一次 RPC 就断）——免去建/删临时队列的开销；
- 请求本就走 MQ、想顺手拿个结果——不值得再引一套 RPC 框架；
- 可接受"丢了就重试"——at-most-once 语义正合适。

### 不该用

- 微服务间常规同步调用——优先 gRPC / Dubbo / HTTP；
- 应答不能丢——direct reply-to 不够，经典 reply 队列也有"处理完没 ack 就宕机"的问题；
- 要强一致的事务性调用——消息中间件不保证即时性、不保证唯一消费；
- 高吞吐打到同一个 client——direct reply-to 零缓冲，扛不住；
- 长耗时任务（几十秒以上）——阻塞等结果占着连接，改异步事件更合理。

---

## 六、运行方式

### 前置条件

本机（或可达的）RabbitMQ Broker，默认连接 `localhost:5672`，账号 `admin/admin`，vhost `/`。
最简单的方式：

```bash
docker run -d --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:3.13-management
```

如需改连接参数，编辑 `src/main/resources/application.yml` 中 `rabbitmq.practice.*`。

### 编译运行

在仓库根目录执行：

```bash
mvn -pl ch17-rpc -am spring-boot:run
```

> 注：该命令要求 `ch17-rpc` 已在父 `pom.xml` 的 `<modules>` 中登记；若尚未登记，也可进入本模块目录直接 `mvn spring-boot:run`。

### 预期输出（节选）

```
========== RabbitMQ RPC 演示（direct reply-to） ==========
[Server 启动] 等待 RPC 请求，队列=rpc.queue
[Client 发送] fib:10 (corrId=...)
[Server 处理] fib:10 -> 55 (replyTo=amq.rabbitmq.reply-to.xxx, corrId=...)
[应答] 请求=fib:10 -> 应答=55
[Client 发送] upper:hello rpc (corrId=...)
[Server 处理] upper:hello rpc -> HELLO RPC (...)
[应答] 请求=upper:hello rpc -> 应答=HELLO RPC
...
========== RPC 演示完成（进程靠 spring.main.keep-alive 保活，Ctrl+C 退出） ==========
```

演示跑完后进程靠 `spring.main.keep-alive: true` 保活，便于在管理界面观察，`Ctrl+C` 退出。

# ch04-queue-concepts —— 队列核心概念

博客《RabbitMQ 系列 · 第 4 篇：队列核心概念——命名、顺序、优先级与策略》配套示例。

用 **原生 amqp-client** 演示 RabbitMQ 队列的四个核心概念：**优先级队列**、**服务端命名队列**、
**临时/独占队列**、**声明等价（PRECONDITION_FAILED）**。

> 这些概念与具体的队列类型（Classic / Quorum / Stream）无关，是所有队列共有的。

---

## ⚠️ 运行前置

需要本机有一个正在运行的 RabbitMQ Broker。按博客 1.1 节用 Docker 拉起：

```bash
docker run -d --name rabbitmq \
  -p 5672:5672 -p 15672:15672 \
  -e RABBITMQ_DEFAULT_USER=admin -e RABBITMQ_DEFAULT_PASS=admin \
  rabbitmq:3.13-management
```

默认连接 `localhost:5672`、用户 `admin/admin`、vhost `/`，可在 `src/main/resources/application.yml`
中修改（博客原文常 `192.168.65.112` + vhost `/mirror`）。

---

## 运行方式

```bash
mvn -pl ch04-queue-concepts -am spring-boot:run
```

启动后由 `QueueConceptsDemoRunner`（`CommandLineRunner`）依次演示四个场景，每步打印清晰标识：

### 场景1：优先级队列

声明 `x-max-priority=10` 的队列，先无消费者发布 5 条不同优先级的消息（发布顺序 priority=1,1,9,5,1），
再挂消费者观察投递顺序。预期投递顺序 `9→5→1→1→1`——**高优先级插队**，同优先级仍 FIFO。

### 场景2：服务端命名队列

`queueDeclare("")` 传空串，由 Broker 生成唯一名（形如 `amq.gen-xxxx`）。随后用 `basicConsume("")`
（空串）注册消费者，Broker 会将其解析为刚才生成的队列名——**Channel 记忆特性**，临时队列无需把名字传来传去。

### 场景3：临时/独占队列

`queueDeclare()` 无参版（= `queueDeclare("", false, true, true, null)`）声明一个服务端命名 + exclusive +
autoDelete 的临时队列。exclusive 队列**连接私有、连接关即删**，连接关闭（try-with-resources）后自动消失。

### 场景4：声明等价（PRECONDITION_FAILED）

注释说明：同名重复声明队列时，`durable / exclusive / autoDelete / arguments` 必须全部一致，否则触发
通道级异常 **406 PRECONDITION_FAILED**。代码里仅打印说明，不真正触发异常（以免中断演示）。

---

## 配置项

| 配置键 | 默认值 | 说明 |
|--------|--------|------|
| `rabbitmq.practice.host` | `localhost` | Broker 地址 |
| `rabbitmq.practice.port` | `5672` | Broker 端口 |
| `rabbitmq.practice.username` | `admin` | 用户名 |
| `rabbitmq.practice.password` | `admin` | 密码 |
| `rabbitmq.practice.virtual-host` | `/` | vhost |

---

## 代码结构

| 类 | 对应博客 | 作用 |
|----|---------|------|
| `QueueConceptsApplication` | — | `@SpringBootApplication` 入口 |
| `config.RabbitMqProperties` | — | `@ConfigurationProperties` 绑定连接参数 |
| `config.RabbitMqConfig` | — | 构建共享的 amqp-client `ConnectionFactory` |
| `QueueConceptsDemoRunner` | 第五/一/六/二节 | `CommandLineRunner`，依次演示四个队列核心概念 |

---

## 注意事项（来自博客）

| 注意点 | 说明 |
|--------|------|
| **x-max-priority 只能声明时设** | policy 改不了；建议优先级数用 1~10 |
| **exclusive 一定是 Classic** | Quorum / Stream 要跨节点复制，生命周期绑死单连接不成立 |
| **exclusive 建议服务端命名** | 避免多条连接抢同一个固定名产生竞态 |
| **PRECONDITION_FAILED** | 若 Broker 上已存在同名队列但参数不一致（如之前手动建过），`queueDeclare` 会报 406，删掉旧队列再跑 |

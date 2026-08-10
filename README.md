# rabbitmq-blog-demo

RabbitMQ 练习项目，对应博客《[RabbitMQ 安装与核心概念——Queue、Exchange、Channel](https://code-corey.github.io/中间件/rabbitmq/rabbitmq-02-install-concepts)》的 **5.1（Maven 依赖）** 与 **5.2（消费者示例）**。

用原生 `com.rabbitmq:amqp-client` 客户端（与博客一致），套一层 Spring Boot 外壳，方便配置与打包。

## 环境

- JDK 17
- Maven 3.6+
- 一个可连接的 RabbitMQ（推荐按博客 1.1 节用 Docker 拉起）：

  ```bash
  docker run -d --name rabbitmq \
    -p 5672:5672 -p 15672:15672 \
    -e RABBITMQ_DEFAULT_USER=admin -e RABBITMQ_DEFAULT_PASS=admin \
    rabbitmq:3.13-management
  ```

## 配置

`src/main/resources/application.yml`：

```yaml
rabbitmq:
  practice:
    host: localhost      # 博客 5.2 原文用 192.168.65.112
    port: 5672
    username: admin
    password: admin
    virtual-host: /      # 博客 5.2 原文用 /mirror
    queue: test2
```

按需改成你自己的 Broker 地址、账号、vhost 与队列名。

## 运行

### 1. 启动消费者（对应博客 5.2 FirstConsumer）

```bash
mvn spring-boot:run
```

看到 `[*] 等待消息于队列 test2` 即开始监听；进程会阻塞等待消息，`Ctrl+C` 退出。

### 2. 另开一个终端，发送一条消息

```bash
mvn spring-boot:run -Dspring-boot.run.arguments=--app.mode=send
```

或先打包再运行：

```bash
mvn -q -DskipTests package
java -jar target/rabbitmq-learning-0.0.1-SNAPSHOT.jar --app.mode=send
```

### 3. 观察输出

生产者侧：

```
[x] 已发送: hello rabbitmq @ 14:30:01.234
```

消费者侧：

```
routingKey > test2
deliveryTag > 1
content: hello rabbitmq @ 14:30:01.234
```

也可以打开管理控制台 [http://localhost:15672](http://localhost:15672)（admin/admin），在 Queues 看 `test2` 的 Ready / Total 变化，在 Connections / Channels 看连接与信道。

## 代码与博客对照

| 博客小节 | 本项目 |
|----------|--------|
| 5.1 Maven 依赖 | `pom.xml` 中的 `com.rabbitmq:amqp-client:5.21.0` |
| 5.2 FirstConsumer | `consumer/FirstConsumer.java`（连接参数改为从 `application.yml` 注入，消费逻辑一致） |
| 3.1 发持久消息 | `producer/FirstProducer.java` 用 `MessageProperties.PERSISTENT_TEXT_PLAIN` |
| `queueDeclare` 五参数 | 两处 `queueDeclare(queue, true, false, false, null)`，详解见博客 5.2 |

## 说明

- 消费者进程由 `spring.main.keep-alive=true` 保活；发送端发完即 `ctx.close()` 退出。
- 手动 ACK：消费者在 `handleDelivery` 里 `basicAck`，`basicQos(1)` 保证一次只投递一条。
- `FirstProducer` 用默认交换机 `""` + routingKey=队列名直接投递，方便在还没讲 Exchange 的阶段先跑通。

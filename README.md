# rabbitmq-blog-demo

RabbitMQ 练习项目（多模块），对应博客 [RabbitMQ 系列](https://code-corey.github.io/中间件/rabbitmq/rabbitmq-02-install-concepts)。每个模块对应一篇博客，原生 API 与 Spring AMQP 对照实现。

## 模块

| 模块 | 对应博客 | 要点 |
|------|---------|------|
| [`ch02-install-concepts`](./ch02-install-concepts) | [02 安装与核心概念](https://code-corey.github.io/中间件/rabbitmq/rabbitmq-02-install-concepts) | 原生 `amqp-client`：5.1 依赖 + 5.2 FirstConsumer |
| [`ch05-springboot`](./ch05-springboot) | [05 SpringBoot 集成](https://code-corey.github.io/中间件/rabbitmq/rabbitmq-05-springboot) | Spring AMQP：`RabbitTemplate` + `@RabbitListener` + Publisher Confirms |

## 环境

- JDK 17
- Maven 3.6+
- 一个可连接的 RabbitMQ（按博客 1.1 节用 Docker 拉起）：

  ```bash
  docker run -d --name rabbitmq \
    -p 5672:5672 -p 15672:15672 \
    -e RABBITMQ_DEFAULT_USER=admin -e RABBITMQ_DEFAULT_PASS=admin \
    rabbitmq:3.13-management
  ```

## 构建

```bash
mvn -DskipTests package     # 构建所有模块
```

---

## ch02-install-concepts（原生 amqp-client）

连接配置：`ch02-install-concepts/src/main/resources/application.yml`（默认 localhost / admin / vhost=`/`；博客原文 192.168.65.112 / `/mirror`）。

```bash
# 消费者（博客 5.2 FirstConsumer）
mvn -pl ch02-install-concepts -am spring-boot:run -Dspring-boot.run.arguments=--app.mode=consumer

# 另开终端发一条
mvn -pl ch02-install-concepts -am spring-boot:run -Dspring-boot.run.arguments=--app.mode=send
```

代码对照：

| 博客小节 | 代码 |
|----------|------|
| 5.1 Maven 依赖 | `pom.xml` 的 `com.rabbitmq:amqp-client:5.21.0` |
| 5.2 FirstConsumer | `consumer/FirstConsumer.java` |
| 3.1 发持久消息 | `producer/FirstProducer.java`（`PERSISTENT_TEXT_PLAIN`） |

---

## ch05-springboot（Spring AMQP）

连接配置：`ch05-springboot/src/main/resources/application.yml`（含 `publisher-confirm-type` / `publisher-returns` / manual ack）。

```bash
mvn -pl ch05-springboot -am spring-boot:run
```

启动后自动：声明 `demo.exchange` / `demo.queue` / 绑定 → `DemoRunner` 发送 3 条消息 → `@RabbitListener` 消费并手动 ACK。日志可见 Publisher Confirm（ACK）与消费输出。

代码对照：

| 博客小节 | 代码 |
|----------|------|
| 二、配置关键参数 | `application.yml`（`spring.rabbitmq.*`） |
| 三、声明 Exchange/Queue/Binding | `config/RabbitConfig.java` |
| 四、RabbitTemplate + Confirms | `producer/DemoProducer.java` |
| 五、@RabbitListener 消费 | `consumer/DemoConsumer.java` |

## 说明

- 两个模块都靠 `keep-alive`（ch05）或连接线程（ch02）保活，`Ctrl+C` 退出。
- ch05 的 `acknowledge-mode: manual` + `prefetch: 1`，对应原生的手动 ACK 与 `basicQos(1)`。
- ch05 的 `DemoRunner` 用 `ApplicationReadyEvent` 触发发送，确保监听容器已就绪再发。

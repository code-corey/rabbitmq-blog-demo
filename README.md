# rabbitmq-blog-demo

RabbitMQ 系列博客的配套练习项目（Maven 多模块），**每篇一个独立 module**。以原生 `com.rabbitmq:amqp-client`（与博客一致）为主，第 5 章用 Spring AMQP。

> 博客：https://code-corey.github.io/中间件/rabbitmq/rabbitmq-01-what-is-mq

## 模块总览

| module | 博客篇 | 要点 | 运行 |
|--------|--------|------|------|
| [`ch01-what-is-mq`](./ch01-what-is-mq) | 01 MQ 是什么 | Spring Boot 进程内 `ApplicationEvent`（不连 Broker，作 MQ 引子） | 无需 Broker |
| [`ch02-install-concepts`](./ch02-install-concepts) | 02 安装与核心概念 | 原生 amqp-client：5.1 依赖 + 5.2 FirstConsumer | 本机 Broker |
| [`ch03-programming-model`](./ch03-programming-model) | 03 基础编程模型 | 七步骨架；**basicGet(拉) vs basicConsume(推)**；alternate-exchange 兜底 | 本机 Broker |
| [`ch04-messaging-patterns`](./ch04-messaging-patterns) | 04 消息场景 | **Hello/Work/Fanout/Direct/Topic/Headers** 六场景 + Publisher Confirms | 本机 Broker |
| [`ch05-springboot`](./ch05-springboot) | 05 SpringBoot 集成 | Spring AMQP：`RabbitTemplate` + `@RabbitListener` + Confirms | 本机 Broker |
| [`ch06-queue-types`](./ch06-queue-types) | 06 队列类型 | **Classic / Quorum / Stream** 声明与消费（Stream 用原生 Channel） | 本机 Broker |
| [`ch07-dlx-delay`](./ch07-dlx-delay) | 07 死信与延迟 | **TTL + DLX** 延迟队列（订单关单链路） | 本机 Broker |
| [`ch08-sharding`](./ch08-sharding) | 08 分片 | Sharding 插件 Producer / Consumer | ⚠️ 需 sharding 插件 + 策略 |
| [`ch09-monitor-backup-federation`](./ch09-monitor-backup-federation) | 09 监控备份联邦 | Federation Up/Down 代码 + 监控/备份文档 | ⚠️ 需 federation 插件 + 双 Broker |
| [`ch10-cluster-ha`](./ch10-cluster-ha) | 10 集群与高可用 | 纯文档（集群/镜像策略命令，无 Java） | 无代码 |
| [`ch11-classic-backlog-degradation`](./ch11-classic-backlog-degradation) | 11 Classic 积压退化 | **自研积压压测 demo**（博客无代码） | 本机 Broker |

## 环境

- JDK 17
- Maven 3.6+
- 本机 RabbitMQ（按博客 1.1 节用 Docker 拉起）：

  ```bash
  docker run -d --name rabbitmq \
    -p 5672:5672 -p 15672:15672 \
    -e RABBITMQ_DEFAULT_USER=admin -e RABBITMQ_DEFAULT_PASS=admin \
    rabbitmq:3.13-management
  ```

## 构建全部模块

```bash
mvn -DskipTests package
```

## 运行单个模块

多数模块：`mvn -pl <module> -am spring-boot:run`，连接参数在各自 `src/main/resources/application.yml`（默认 `localhost:5672 / admin / admin / vhost=/`；博客原文常 `192.168.65.112 / /mirror`，按需改）。

```bash
# ch05：Spring AMQP 收发闭环（启动自动发 3 条 + 消费）
mvn -pl ch05-springboot -am spring-boot:run

# ch02：原生 amqp-client，consumer / send 两种模式
mvn -pl ch02-install-concepts -am spring-boot:run -Dspring-boot.run.arguments=--app.mode=consumer
mvn -pl ch02-install-concepts -am spring-boot:run -Dspring-boot.run.arguments=--app.mode=send

# ch04：六种消息场景依次演示
mvn -pl ch04-messaging-patterns -am spring-boot:run

# ch11：Classic 积压压测（高速 producer + 慢 consumer，观察吞吐断崖）
mvn -pl ch11-classic-backlog-degradation -am spring-boot:run
```

## 各模块说明

- **ch01** 不连 RabbitMQ，是 Spring 进程内事件 demo，作 MQ 引子。
- **ch02/03/04/06/07/08/09/11** 用原生 `com.rabbitmq:amqp-client`（与博客一致）。
- **ch05** 用 Spring AMQP（`spring-boot-starter-amqp`）。
- **ch10** 无 Java 代码，是集群运维文档 module（pom + README）。
- **ch11** 代码为自研压测演示；博客原文是纯性能分析、无代码。
- **ch08 / ch09** 代码可编译运行，但实跑需额外插件 / 多 Broker，详见各自 README。

## 备注

- 多个模块用 `spring.main.keep-alive: true` 保活，`Ctrl+C` 退出。
- 连接参数全部可配（`application.yml`）；博客原文的 `192.168.65.112` / `/mirror` 已改为本地默认，注释里说明。
- 若 Broker 上已存在同名队列但参数不一致（如之前手动建过），`queueDeclare` 会报 `PRECONDITION_FAILED`，删掉旧队列再跑即可。

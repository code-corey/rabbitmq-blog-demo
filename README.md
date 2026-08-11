# rabbitmq-blog-demo

RabbitMQ 系列博客的配套练习项目（Maven 多模块），**博客每篇一个独立 module**（运维/配置类篇章除外）。以原生 `com.rabbitmq:amqp-client`（与博客一致）为主，第 6 篇用 Spring AMQP。

> 博客：https://code-corey.github.io/中间件/rabbitmq/rabbitmq-01-what-is-mq

## 模块总览

| module | 博客篇 | 要点 | 运行 |
|--------|--------|------|------|
| [`ch01-what-is-mq`](./ch01-what-is-mq) | 01 MQ 是什么 | Spring Boot 进程内 `ApplicationEvent`（不连 Broker） | 无需 Broker |
| [`ch02-install-concepts`](./ch02-install-concepts) | 02 安装与核心概念 | 原生 amqp-client：5.1 依赖 + 5.2 FirstConsumer | 本机 Broker |
| [`ch03-programming-model`](./ch03-programming-model) | 03 基础编程模型 | 七步骨架；basicGet 拉 vs basicConsume 推；AE 兜底 | 本机 Broker |
| [`ch04-queue-concepts`](./ch04-queue-concepts) | 04 队列核心概念 | 优先级队列、服务端命名、临时/独占、声明等价 | 本机 Broker |
| [`ch05-messaging-patterns`](./ch05-messaging-patterns) | 05 消息场景 | **六场景 + Confirms**（Hello/Work/Fanout/Direct/Topic/Headers） | 本机 Broker |
| [`ch06-springboot`](./ch06-springboot) | 06 SpringBoot 集成 | Spring AMQP：`RabbitTemplate` + `@RabbitListener` + Confirms | 本机 Broker |
| [`ch07-queue-types`](./ch07-queue-types) | 07 队列类型 | Classic / Quorum / Stream 声明与消费 | 本机 Broker |
| [`ch08-dlx-delay`](./ch08-dlx-delay) | 08 死信与延迟 | TTL + DLX 延迟队列（订单关单链路） | 本机 Broker |
| [`ch09-sharding`](./ch09-sharding) | 09 分片 | Sharding 插件 Producer / Consumer | ⚠️ 需 sharding 插件 |
| [`ch10-monitor-backup-federation`](./ch10-monitor-backup-federation) | 10 监控备份联邦 | Federation Up/Down 代码 + 监控/备份文档 | ⚠️ 需 federation 插件+双 Broker |
| [`ch11-cluster-ha`](./ch11-cluster-ha) | 11 集群与高可用 | 纯文档（集群/镜像策略命令，无 Java） | 无代码 |
| [`ch12-classic-backlog-degradation`](./ch12-classic-backlog-degradation) | 12 Classic 积压退化 | Classic 积压压测自研 demo | 本机 Broker |
| [`ch13-amqp-and-protocols`](./ch13-amqp-and-protocols) | 13 多协议 | AMQP 0-9-1 基线 demo + 协议文档（MQTT/STOMP/Stream） | 本机 Broker |
| [`ch15-security`](./ch15-security) | 15 安全 | TLS 连接 demo + 认证/授权文档 | 普通/TLS Broker |
| [`ch16-virtual-hosts`](./ch16-virtual-hosts) | 16 Virtual Hosts | 连接指定 vhost 收发 + vhost 文档 | 本机 Broker |
| [`ch17-rpc`](./ch17-rpc) | 17 RPC | direct-reply-to RPC client/server | 本机 Broker |

> **跳过的篇章**（运维/配置为主，无 Java 代码，未建 module）：14 网络与连接、18 Shovel、19 常用插件、20 Peer Discovery、21 升级迁移、22 生产实践——见对应博客篇即可。

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

多数模块：`mvn -pl <module> -am spring-boot:run`，连接参数在各自 `src/main/resources/application.yml`（默认 `localhost:5672 / admin / admin / vhost=/`）。

```bash
# ch05：六种消息场景依次演示（启动自动跑通 Hello/Work/Fanout/Direct/Topic/Headers + Confirms）
mvn -pl ch05-messaging-patterns -am spring-boot:run

# ch06：Spring AMQP 收发闭环
mvn -pl ch06-springboot -am spring-boot:run

# ch17：direct-reply-to RPC（server + client 同进程来回）
mvn -pl ch17-rpc -am spring-boot:run
```

## 说明

- **ch01–05、07–10、12–13、15–17** 用原生 `com.rabbitmq:amqp-client`（与博客一致）；**ch06** 用 Spring AMQP。
- **ch11** 无 Java 代码，是集群运维文档 module；**ch12** 代码为自研压测（博客原文无代码）。
- **ch09 / ch10** 代码可编译运行，但实跑需额外插件 / 多 Broker，详见各自 README。
- 多个模块用 `spring.main.keep-alive: true` 保活，`Ctrl+C` 退出。
- 若 Broker 上已存在同名队列但参数不一致，`queueDeclare` 会报 `PRECONDITION_FAILED`，删掉旧队列再跑。

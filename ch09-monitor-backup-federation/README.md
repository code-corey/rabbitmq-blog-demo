# ch09-monitor-backup-federation

博客《RabbitMQ 监控、备份与联邦同步》第 09 篇示例 —— 原生 amqp-client 实现 Federation 联邦同步。

本篇只有 Federation 一节有 Java 代码（下游消费者），监控（HTTP Management API）与备份部分博客无 Java，步骤见本文档末尾。

## 模块结构

```
src/main/java/io/github/codecorey/federation
├── FederationApplication.java      # @SpringBootApplication 入口
├── config/
│   └── FederationProperties.java   # @ConfigurationProperties("app")，两组 broker 配置
├── consumer/
│   └── DownStreamConsumer.java     # 连【下游】，exchangeDeclare/queueDeclare/queueBind + basicConsume
└── producer/
    └── UpStreamProducer.java       # 向【上游】fed_exchange 发若干消息（联邦消息源）
```

## 运行方式

启动后同时运行两个 `CommandLineRunner`（`@Order` 控制先后）：

1. `DownStreamConsumer`（Order 1）：连下游 Broker，声明 `fed_exchange` / `fed_queue` 并绑定，`basicConsume(autoAck=true)` 监听。
2. `UpStreamProducer`（Order 2）：等待 `app.producer-delay-ms` 后，连上游 Broker，向 `fed_exchange` 发 `app.producer-count` 条持久化消息，发送完关闭自身连接。

下游消费者连接不关闭，由 `spring.main.keep-alive: true` 保活持续消费。日志用 SLF4J 输出。

```bash
mvn -pl ch09-monitor-backup-federation spring-boot:run
# 或打包后
mvn -pl ch09-monitor-backup-federation package
java -jar ch09-monitor-backup-federation/target/ch09-monitor-backup-federation-0.0.1-SNAPSHOT.jar
```

> 单机自测（无 federation）也能编译运行：upstream/downstream 都指向同一个 localhost:5672，
> 此时 `DownStreamConsumer` 直接在本机消费，`UpStreamProducer` 发出的消息会被本机消费者收到（不经联邦链路）。

## 前置：双 Broker + Federation 插件

代码能编译即可运行；**实跑联邦需双 Broker 并启用 federation 插件**。

### 1. 两个 Broker（不同主机/端口/容器）

- 上游（消息源）：博客中为 `192.168.65.193`
- 下游（消费者本地）：博客中为 `192.168.65.112`
- 修改 `application.yml` 把 `app.upstream.*` 指向上游、`app.downstream.*` 指向下游
- **DownStream 与 UpStream 建议使用相同 Virtual Host**

### 2. 启用 federation 插件（下游 Broker）

```bash
rabbitmq-plugins enable rabbitmq_federation
rabbitmq-plugins enable rabbitmq_federation_management
```

启用后管理控制台 Admin 菜单新增 **Federation Status**、**Federation Upstreams**。

### 3. 下游主动配 upstream URI

Federation 由**下游主动**连接上游。在**下游**管理控制台 **Admin → Federation Upstreams → Add** 填入：

```
amqp://admin:admin@192.168.65.193:5672/
```

> URI 中已含 vhost（末尾 `/`），表单里的 Virtual Host **不要再重复配置**。

### 4. 配置 Federation Policy

在**下游** **Admin → Policies** 新建策略，Definition 至少指定一个目标：

| 参数 | 说明 |
|------|------|
| `federation-upstream` | 对单个 Upstream 生效 |
| `federation-upstream-set` | 对一组 Upstream 生效，`all` 表示全部 |

Apply to 选 Exchanges（或 Queues），Pattern 匹配 `fed_exchange`。

### 5. 验证

- **Federation Status** 显示 `running` 即成功；失败会给出原因
- 在上游 `fed_exchange` 发消息（本模块 `UpStreamProducer`），下游本地 `fed_queue` 的消费者（`DownStreamConsumer`）应能收到
- 上游会看到联邦交换机及默认 routing key 绑定

---

## 监控：HTTP Management API

管理控制台 Overview 适合人工巡检；对接 Prometheus / Grafana / 自建告警用 HTTP API。

控制台底部集成 API 文档。常用入口（需启用 `rabbitmq_management` 插件，默认端口 15672）：

```
GET http://<server>:15672/api/overview          # 系统资源、对象计数、消息统计
```

其他常用接口：

| 接口 | 用途 |
|------|------|
| `GET /api/queues` | 队列列表与积压 |
| `GET /api/nodes` | 节点状态 |
| `GET /api/connections` | 连接详情 |
| `GET /api/exchanges` | 交换机列表 |
| `GET /api/channels` | 通道详情 |

调用示例（HTTP Basic 认证，监控建议用独立最小权限账号）：

```bash
curl -u admin:admin http://localhost:15672/api/overview | jq
curl -u admin:admin http://localhost:15672/api/queues | jq
```

对接 **Prometheus** 用 `rabbitmq_exporter`（或新版 RabbitMQ 自带 prometheus 插件），再由 **Grafana** 仪表盘展示。

生产环境须：

- 限制 API 端口访问（防火墙 / 内网）
- 使用独立监控账号，最小权限
- 启用 HTTPS（反向代理或 TLS）

## 备份：definitions 导出与消息

### 元数据：JSON 导入导出

Web 控制台 **Admin → Export definitions / Import definitions** 可导出、导入 JSON 元数据
（Exchange、Queue、Binding、用户、策略等结构定义）。迁移集群或灾难恢复时先在新环境导入 definitions。

### 消息：文件级备份

RabbitMQ 数据目录默认 `/var/lib/rabbitmq/mnesia`：

| 部分 | 内容 |
|------|------|
| 元数据 | Exchange、Queue、Binding、用户、策略等结构定义（用上面的 JSON 导出） |
| 消息存储 | 持久化消息体（按 vhost 组织） |

MQ 消息**一般不建议**像数据库那样频繁冷备（业务上更依赖集群冗余与 Confirms）。若必须备份：

1. **停止应用**（镜像集群需**整集群停服**）
2. 复制 vhost 对应目录：

   ```
   /var/lib/rabbitmq/mnesia/rabbit@node-name/msg_stores/vhosts/
   ```
3. 目标节点已导入相同元数据后，按 vhost 复制文件夹；持久化与非持久化消息一并复制

恢复后启动集群，验证队列深度与消费是否正常。

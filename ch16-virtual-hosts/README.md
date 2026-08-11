# ch16-virtual-hosts · RabbitMQ Virtual Hosts——隔离、权限与配额

博客《RabbitMQ 系列 · 第 16 篇：Virtual Hosts——隔离、权限与配额》配套示例。

用 **原生 amqp-client** 演示：通过 `ConnectionFactory.setVirtualHost(...)` 连接到指定 vhost，
声明队列、收发一条消息，并打印当前 vhost。核心体现「**换 vhost 只需改配置**」。

---

## 0. Virtual Host 是什么

**vhost（虚拟主机）是 RabbitMQ 多租户隔离的核心单位**——一套集群里划出多个互不相干的资源空间：

- **资源隔离**：每个 vhost 拥有独立的一套 Exchange / Queue / Binding / Policy / Runtime 参数。
  A vhost 的 `orders.fanout` 和 B vhost 的同名交换机是两条完全无关的实体，**可重名**。
- **权限隔离**：用户**没有"全局权限"**，只有在一个或多个 vhost 内的权限
  （configure / write / read 三维正则，详见博客第四节）。

> vhost 提供**逻辑隔离，不是物理隔离**——同一集群的 CPU / 磁盘 / 网络是共享的。
> 要硬隔离，要么靠配额（见下文），要么拆集群。

类比 Apache 的 virtual host / Nginx 的 server block，但**关键差别**：
Apache vhost 写在配置文件里靠重载生效；**RabbitMQ 的 vhost 用命令现建现删**
（`rabbitmqctl` 或 HTTP API），属于运行时动态资源。

默认 vhost 名为 **`/`（正斜杠）**，默认用户 `guest`/`guest` 就活在它里面。

---

## 1. 运行前置

### 方式 A：直接用默认 vhost `/`（开箱即用）

默认 vhost `/` RabbitMQ 启动即自带，本 module 默认配置 `virtual-host: /`、用户 `admin/admin`。
只要 Broker 里 `admin` 用户对 `/` 有权限（或直接用 `guest/guest`），**无需任何前置**，直接运行。

### 方式 B：用自定义 vhost（如 `/mirror`）

切换到自定义 vhost 前，**必须先在 Broker 建好该 vhost 并给用户授权**，否则连接会被拒
（`ACCESS_REFUSED - access to vhost '/mirror' refused`）：

```bash
# 1. 建 vhost
rabbitmqctl add_vhost /mirror

# 2. 给 admin 在 /mirror 内授权（configure write read 三维正则，全允许用 .*）
rabbitmqctl set_permissions -p /mirror admin ".*" ".*" ".*"

# 3.（可选）给 vhost 上配额护栏：最多 256 连接、1024 队列
rabbitmqctl set_vhost_limits -p /mirror '{"max-connections": 256, "max-queues": 1024}'
```

然后把 `application.yml` 的 `rabbitmq.vhost.virtual-host` 从 `/` 改成 `/mirror`，
**代码一行不用动**——这就是 vhost 作为配置切换点的价值。

> 新建的 vhost 是"空壳"：自带默认交换机（`amq.` 前缀），但**没有任何用户权限**，
> 必须显式 `set_permissions`，否则连接被拒。

---

## 2. 常用 rabbitmqctl 命令速查

### 2.1 vhost 增删查

```bash
# 建（可带元数据：描述、默认队列类型、标签）
rabbitmqctl add_vhost qa1 --description "QA env 1" --default-queue-type quorum --tags qa,project-a

# 列出
rabbitmqctl list_vhosts
rabbitmqctl -q --formatter=pretty_table list_vhosts name description tags default_queue_type

# 改元数据（不改名）
rabbitmqctl update_vhost_metadata qa1 --description "..." --tags qa,project-a

# 删（级联销毁内部所有实体，谨慎！）
rabbitmqctl delete_vhost qa1

# 删除保护（生产建议给关键 vhost 开）
rabbitmqctl enable_vhost_protection_from_deletion "prod-orders"
rabbitmqctl disable_vhost_protection_from_deletion "prod-orders"
```

### 2.2 权限（per-vhost，三维正则）

```bash
# 在 qa1 里给 app-prod 全权限（configure write read）
rabbitmqctl set_permissions -p qa1 app-prod ".*" ".*" ".*"

# 只读（消费侧）：configure/write 留空 ^$，read 限 events 前缀
rabbitmqctl set_permissions -p qa1 consumer-only "^$" "^$" "^events\."

# 清空某用户在该 vhost 的所有权限
rabbitmqctl clear_permissions -p qa1 consumer-only
```

> 不带 `-p` 时 `set_permissions` 作用于默认 vhost `/`。没有"全局权限"。

### 2.3 配额（max-connections / max-queues）

```bash
# 限制并发连接数 / 队列数
rabbitmqctl set_vhost_limits -p qa1 '{"max-connections": 256, "max-queues": 1024}'

# max-connections 设 0 = 临时封死该 vhost（不删数据，客户端一连接就被拒）
rabbitmqctl set_vhost_limits -p qa1 '{"max-connections": 0}'

# 查看 / 清除
rabbitmqctl list_vhost_limits -p qa1
rabbitmqctl clear_vhost_limits -p qa1
```

---

## 3. 两个关键坑

### 3.1 URI 里默认 vhost `/` 要转义成 `%2F`

AMQP URI 最后一段是 vhost 名。**默认 `/` 在 URI 里必须写成 `%2F`**：

```
amqp://guest:guest@localhost:5672/%2F      # 正确：连接默认 vhost /
amqp://guest:guest@localhost:5672/         # 错误：会被当成空路径
amqp://app:pwd@rabbitmq.local:5672/qa1     # 自定义 vhost 直接写名字
```

本 module 用 `ConnectionFactory.setVirtualHost(props.getVirtualHost())`，
配置里 `virtual-host: /` 原样写 `/` 即可（不走 URI 解析，无转义问题）。

### 3.2 默认队列类型（DQT）

vhost 元数据里可设 `--default-queue-type`（quorum / stream / classic），优先级：
**vhost 级 > 节点级（`rabbitmq.conf` 的 `default_queue_type`）> classic 兜底**。

- **只对新声明生效**，已存在的队列类型不可变（声明等价铁律仍成立）。
- 从 classic 迁移到 quorum 期间，可临时开
  `quorum_queue.property_equivalence.relaxed_checks_on_redeclaration = true`，迁完即关。

---

## 4. 运行方式

默认连接 `localhost:5672`、用户 `admin/admin`、vhost `/`，可在 `src/main/resources/application.yml`
中修改（含切换到自定义 vhost）。

```bash
mvn -pl ch16-virtual-hosts spring-boot:run
```

启动后日志会打印当前 vhost，在对应 vhost 中声明 `vhost.demo.queue` 队列，发一条消息并由
Consumer 接收打印。非 Web 应用，靠 `spring.main.keep-alive: true` 保活，Ctrl+C 退出。

### 体验「换 vhost 只需改配置」

1. 先按上面「方式 B」建好 `/mirror` 并给 `admin` 授权；
2. 把 `application.yml` 的 `virtual-host: /` 改成 `virtual-host: /mirror`；
3. 重新运行——日志会显示连接到 `/mirror`，队列声明在 `/mirror` 里，与 `/` 互不相干。

---

## 5. 配置项

| 配置键 | 默认值 | 说明 |
|--------|--------|------|
| `rabbitmq.vhost.host` | `localhost` | Broker 地址 |
| `rabbitmq.vhost.port` | `5672` | Broker 端口 |
| `rabbitmq.vhost.username` | `admin` | 用户名 |
| `rabbitmq.vhost.password` | `admin` | 密码 |
| `rabbitmq.vhost.virtual-host` | `/` | **vhost，可改成自定义如 `/mirror`（需先建好并授权）** |
| `rabbitmq.vhost.queue` | `vhost.demo.queue` | 演示用队列名 |

---

## 6. 代码结构

| 类 | 作用 |
|----|------|
| `VhostsApplication` | `@SpringBootApplication` 入口 |
| `config.RabbitMqProperties` | `@ConfigurationProperties` 绑定 `rabbitmq.vhost.*`，含 virtualHost |
| `runner.VhostDemoRunner` | 连接指定 vhost → 声明队列 → 注册 Consumer → 发一条消息，全程打印当前 vhost |

---

## 7. 小结

- **vhost = 资源（Exchange/Queue/Binding/Policy）+ 权限的逻辑隔离单元**，用命令动态建删，非物理隔离。
- **默认 vhost `/`**，URI 里要转义成 `%2F`；生产建议建带语义名的 vhost，别直接用 `/`。
- **管理全靠命令**：`add_vhost` / `set_permissions -p` / `set_vhost_limits -p`；删除级联销毁，关键 vhost 开删除保护。
- **配额**：`max-connections`（设 0 可临时封死）/ `max-queues`。
- **DQT**：vhost 级 > 节点级 > classic；只对新声明生效。
- **换 vhost 只需改配置**——本 module 改一行 `virtual-host` 即在另一个隔离空间里运行。

---

对应博客：[《Virtual Hosts——隔离、权限与配额》](https://code-corey.github.io/中间件/rabbitmq/rabbitmq-16-virtual-hosts)。

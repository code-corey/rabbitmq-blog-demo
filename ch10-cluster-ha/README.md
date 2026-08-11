# ch10-cluster-ha · RabbitMQ 集群与高可用

> **说明**：本篇无 Java 代码，本 module 为 **运维文档** module，不包含 `src` 目录与任何 Java 类。
> 对应博客：[《RabbitMQ 集群与高可用》](https://langkemaoxin.github.io/中间件/rabbitmq/rabbitmq-10-cluster-ha)。
>
> 父项目 `pom.xml` 已将本 module 列入 reactor，仅为在多模块结构中占一个篇章位置；
> `pom.xml` 未声明任何依赖、未挂 `spring-boot-maven-plugin`（无主类，repackage 会失败）。

---

## 0. 为什么要集群

单机 RabbitMQ 宕机可重启；磁盘损坏则 Queue 上的消息可能永久丢失——生产环境不可接受。RabbitMQ 从设计之初就支持 **集群**：

- **普通集群**：各节点共享元数据，消息单份
- **镜像集群（HA）**：消息主动冗余 + 自动选主
- **HAProxy + Keepalived**：对客户端隐藏节点故障（入口高可用）

---

## 1. 集群机制概览

Admin → **Cluster** 可查看集群名（默认 `rabbit@hostname`），单机也是一个单节点集群。

RabbitMQ 提供两种集群模式：

| 模式 | 元数据 | 消息 | 可靠性 | 适用 |
|------|--------|------|--------|------|
| **普通集群** | 各节点相同 | 只存一份，消费时可能跨节点拉取 | 较低，节点挂则该节点消息暂不可消费 | 对安全要求不高的场景 |
| **镜像集群（HA）** | 各节点相同 | 主动同步到镜像节点，选举 master/slave | 高，master 挂自动选主 | 生产推荐 |

---

## 2. 普通集群

准备三台服务器 `worker1`、`worker2`、`worker3`，分别安装 RabbitMQ。

### 2.1 `/etc/hosts` 三节点

各节点解析彼此的主机名，集群节点间才能互相寻址：

```bash
vi /etc/hosts
192.168.65.193  worker1
192.168.65.112  worker2
192.168.65.170  worker3
```

> 节点名建议为 `rabbit@worker1` 形式，与 hostname 对应。博客原文示例使用了 IP 别名（`192-168-65-193`），实际请按机器 hostname 统一，确保后续 `cluster_status` 显示预期成员。

### 2.2 同步 `.erlang.cookie` 并 `chmod 400`

集群各节点的 `/var/lib/rabbitmq/.erlang.cookie` 内容 **必须一致**，否则节点间握手失败。把任一节点的 cookie 复制到其余节点后，统一权限：

```bash
chown rabbitmq:rabbitmq /var/lib/rabbitmq/.erlang.cookie
chmod 400 /var/lib/rabbitmq/.erlang.cookie
```

`chmod 400`（仅属主可读）是 RabbitMQ 对 cookie 文件的硬性要求，权限过宽会拒绝启动。

### 2.3 加入集群：`stop_app` / `join_cluster --ram` / `start_app`

worker1 服务正常后，在 **worker2** 上执行：

```bash
# 停掉本节点 RabbitMQ 应用（Erlang 节点仍在运行）
rabbitmqctl stop_app

# 加入 worker1 所在集群，--ram 表示本节点作为 RAM 节点
rabbitmqctl join_cluster --ram rabbit@worker1

# 重新启动本节点应用，作为集群成员上线
rabbitmqctl start_app
```

> ⚠️ **要点批注**：博客示例命令写作 `join_cluster --ram rabbit@worker2`，这是 **在 worker2 上执行却 join 自己** 的笔误。正确写法是在待加入节点（worker2）上 join 一个 **已在集群中的节点**（如 `rabbit@worker1`）。worker3 同理 join 已有集群。若三个节点都需要加入，最后一个保留 disk 角色、其余按需选 `--ram`。

### 2.4 Disk 节点 vs RAM 节点

| 类型 | 元数据存储 | 特点 |
|------|------------|------|
| **disk** | 硬盘 | 元数据更安全，官方更推荐 |
| **ram** | 内存 | 元数据操作更快，节点全为 ram 可能导致元数据丢失、集群无法启动 |

> **要点批注（重要）**：
> - `--ram` 只影响 **元数据**（Exchange、Queue 定义、Binding、vhost 等），**不影响消息**存储位置。消息按队列所在节点存储，与 disk/ram 无关。
> - 若全部节点都为 ram，重启后元数据全丢、集群无法启动——**生产至少保留一个 disk 节点**。
> - 若某节点是 **唯一 disk 节点**，它宕机即丢失元数据持久化能力，存在单点元数据风险。
> - **生产建议奇数节点**（如 3、5），对 Quorum 队列的多数派投票更友好。

### 2.5 查看集群状态

```bash
rabbitmqctl cluster_status
```

输出会列出 `Nodes`（disk/ram 角色标注）、`Running Nodes`、`Versions` 等。Web 控制台同样可看到多节点拓扑。

---

## 3. 镜像集群（HA）

在普通集群基础上，针对 vhost 配置 **镜像策略（policy）**。

### 3.1 创建 vhost 并下发策略

```bash
# 建一个独立 vhost，便于隔离镜像策略
rabbitmqctl add_vhost /mirror

# 对 /mirror 下所有队列（pattern "^"）启用 ha-mode=all
rabbitmqctl set_policy ha-all --vhost "/mirror" "^" '{"ha-mode":"all"}'
```

也可在 Web 控制台 **Admin → Policies** 配置。配置后该 vhost 下的队列会显示镜像副本与同步状态。

### 3.2 `ha-mode` 参数

| ha-mode | 说明 |
|---------|------|
| **all** | 镜像到集群所有节点；新节点加入时队列同步到新节点（生产常用） |
| **exactly** | 配合数字 `ha-params`，镜像到指定数量节点；节点不足则镜像到全部 |
| **nodes** | 配合节点名列表，镜像到指定节点 |

- **pattern**：队列名匹配规则，`^` 表示全部；通常用 vhost 隔离即可，不必在 pattern 上做精细匹配。
- 镜像模式 **消耗集群内带宽**（每条消息在主从间复制），队列数量不宜过多，尽量避免大量消息长期堆积。

配置完成后，向任一节点发送消息，会同步到其他镜像节点。

---

## 4. HAProxy + Keepalived（了解层）

镜像集群解决了 **数据冗余**，但客户端仍可能连到 **已宕节点**，需要切换连接地址。前端再加一层入口高可用。

### 4.1 HAProxy

在 RabbitMQ 集群前部署 **HAProxy**（TCP 负载均衡）：

- 应用只连 HAProxy 端口，HAProxy 把 AMQP 请求转发到后端健康节点
- 某 RabbitMQ 节点崩溃时，HAProxy 自动剔除并切到其他节点，应用 **无需改 IP**

同类工具：Nginx Stream、F5 等硬件/软件负载均衡。

### 4.2 Keepalived

HAProxy 自身也可能单点。**Keepalived** 暴露 **VIP（虚拟 IP）**：

- VIP 绑定到主 HAProxy 网卡，备 HAProxy 待机
- 主 HAProxy 故障时 VIP **漂移** 到备机
- 应用始终访问同一 VIP，感知不到切换

> HAProxy + Keepalived 是分布式场景的常见组合，部署为「下载 + 配置 + 运行」三步，具体配置细节可参考社区文档与官方运维指南。本篇只到「了解」层。

---

## 5. 普通集群 vs 镜像集群：一句话对比

| 维度 | 普通集群 | 镜像集群（HA） |
|------|----------|----------------|
| **元数据** | 各节点相同 | 各节点相同 |
| **消息** | 单份，消费时可能跨节点拉取 | 主动同步冗余，master/slave 选举 |
| **节点故障影响** | 该节点上的消息暂不可消费 | master 挂自动选主，业务基本无感 |
| **资源开销** | 低 | 高（带宽 + 多副本存储） |
| **定位** | 对安全要求不高的场景 | 生产推荐基线 |

---

## 6. 完整可靠性链路

集群与高可用只是其中一环。生产环境完整可靠性链路通常组合使用：

- **镜像集群 / Quorum 队列**：消息冗余与自动选主
- **Publisher Confirms**：保证消息成功到达 Broker
- **手动 Ack**：保证消费成功后才从队列移除
- **死信队列（DLX）**：消费失败的消息有归宿
- **HAProxy + Keepalived**：对客户端透明的高可用入口

---

## 7. 小结

- **普通集群**：元数据共享，消息单份，节点故障影响该节点上的消息消费
- **镜像集群**：消息冗余 + 自动选主，生产基线
- **HAProxy + Keepalived**：对客户端透明的高可用入口
- **要点批注回顾**：
  - `--ram` 只影响元数据，不影响消息存储
  - 全 ram 节点有元数据丢失风险，至少留一个 disk
  - 生产建议奇数节点，对 Quorum 队列更友好

---

*本 module 仅承载运维文档，无 Java 源码；如需运行集群实验，请按本文命令在真实或虚拟的三台 Linux 上执行。*

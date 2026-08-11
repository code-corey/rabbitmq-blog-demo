# ch15-security

对应博客：[《RabbitMQ 安全——认证、授权与 TLS》](/中间件/rabbitmq/rabbitmq-15-security)

本 module 用原生 `amqp-client` 写一个 **TLS（amqps）连接 demo**：根据 `app.tls.enabled` 开关，走普通 AMQP（5672）或 amqps（5671），连上 Broker 发一条消息，直观演示「链路加密」的切换。连接失败（默认 Docker 镜像未开 TLS）会在日志里打印清晰提示而非抛栈。

---

## 一、AuthN vs AuthZ：先分清两个概念

| 概念 | 全称 | 回答的问题 | 举例 |
|------|------|-----------|------|
| **AuthN** | Authentication（认证） | **你是谁？** | 用户名密码对不对、证书是否可信、JWT 签名是否有效 |
| **AuthZ** | Authorization（授权） | **你能干什么？** | 能不能访问这个 vhost、能不能往这个 exchange 发消息 |

一句话：**认证验明正身，授权划定边界**。客户端连接时先过认证关（亮出凭证），认证通过后再按身份查权限（每个操作都要校验）。

RabbitMQ 把这两层都做成**可插拔的后端**，可自由组合——这正是它安全体系灵活的地方。

---

## 二、内置用户库（Internal Backend）

### 2.1 默认的 guest 用户

节点首次启动（空数据库）时自动创建：vhost `/` + 用户 `guest`（密码 `guest`，全部权限）。

> **关键限制**：`guest` **默认只能从本机连接**，远程连接会被拒。由 `loopback_users` 控制（默认只含 `guest`）。**切勿**把它设成 `none` 来放开 guest 远程访问——正确做法是**新建独立用户、删掉/改密码 guest**。

### 2.2 创建用户与设置权限（rabbitmqctl）

```bash
# 1. 创建用户（交互式输入密码）
rabbitmqctl add_user "app-user"

# 或直接带密码（注意 shell 转义：!、&、$、# 等需转义）
rabbitmqctl add_user "app-user" "2a55f70a841f18b97c3a7db939b7adc9e34a0f1b"

# 2. 授予 vhost 权限（三个正则分别是 configure / write / read）
rabbitmqctl set_permissions -p "custom-vhost" "app-user" ".*" ".*" ".*"

# 3. （可选）打标签，控制管理界面访问权限
rabbitmqctl set_user_tags "app-user" "management"

# 4. 列出所有用户
rabbitmqctl list_users

# 5. 删除用户（会同时关闭该用户的所有连接）
rabbitmqctl delete_user "app-user"
```

> **新建用户必须授权**：刚 `add_user` 出来的用户没有任何 vhost 权限，连接会被拒（`access to vhost '/' refused`）。别忘了 `set_permissions`。

### 2.3 三类权限：configure / write / read

| 权限 | 含义 | 典型操作 |
|------|------|---------|
| **configure** | 创建 / 删除 / 修改资源 | `queue.declare`、`exchange.declare`、`queue.delete` |
| **write** | 往资源里**写**消息 | `basic.publish`（往 exchange 发）、`queue.bind`（绑定 queue） |
| **read** | 从资源里**读**消息 | `basic.get`、`basic.consume`、`queue.purge` |

权限是 **per-vhost** 的——同一个用户在不同 vhost 里可以有完全不同的权限三元组：

```bash
# 格式：set_permissions [-p vhost] 用户  configure  write  read
# 下面表示：在 orders 这个 vhost 里，app-user 只能操作名字以 orders. 开头的资源
rabbitmqctl set_permissions -p "orders" "app-user" "^orders\." "^orders\." "^orders\."

# 查看权限 / 清除权限
rabbitmqctl list_permissions --vhost /
rabbitmqctl list_user_permissions "app-user"
rabbitmqctl clear_permissions -p "orders" "app-user"
```

> **常用正则**：`.*`（全放权）、`^$`（禁止一切）、`^(amq\.gen.*|amq\.default)$`（仅默认资源）。

### 2.4 用户标签（管理界面）

| 标签 | 管理界面权限 |
|------|-------------|
| `management` | 能登录，看自己有权限的 vhost |
| `policymaker` | management + 能设 policy / 参数 |
| `monitoring` | 能看全局统计、所有 vhost |
| `administrator` | 能管理用户、vhost、权限（最高） |

> 标签**不影响消息收发权限**，只管管理界面。

---

## 三、认证与授权后端

用 `auth_backends` 决定用哪些后端、以什么顺序尝试：

| 别名 | 模块 | 提供能力 |
|------|------|---------|
| `internal` | `rabbit_auth_backend_internal` | 认证 + 授权（内置用户库，默认） |
| `ldap` | `rabbit_auth_backend_ldap` | 认证 + 授权 |
| `oauth2` / `oauth` | `rabbit_auth_backend_oauth2` | 授权（认证靠 JWT 自验签） |
| `http` | `rabbit_auth_backend_http` | 认证 + 授权（回调你的 HTTP 接口） |

```ini
# 组合 1：只用内置库（默认）
auth_backends.1 = internal

# 组合 2：先查 LDAP，查不到回退到内置库
auth_backends.1 = ldap
auth_backends.2 = internal

# 组合 3：LDAP 做认证、内置库做授权（混合模式）
auth_backends.1.authn = ldap
auth_backends.1.authz = internal
```

> **链式回退**：多个认证后端时，第一个返回成功的即为最终结果；混合模式可「一个后端验身份、另一个查权限」。

**LDAP**（`rabbitmq-plugins enable rabbitmq_auth_backend_ldap`）适合与企业目录服务打通；**务必加 cache 后端**（`rabbitmq_auth_backend_cache`）缓存 15~60s，否则每次认证都打网络。

**OAuth2** 让客户端用 **JWT access token** 认证（token 作为密码字段传入，用户名被忽略），RabbitMQ 本地验签、解析 scope 翻译成权限。适合 K8s、与 Keycloak / Auth0 / Entra ID 等对接。

**x509 证书认证（EXTERNAL 机制）**：上了 mTLS 后，可直接用客户端证书身份做认证，连密码都省了——见下文 mTLS。

---

## 四、TLS / SSL：加密链路

### 4.1 为什么要 TLS

不用 TLS 时，AMQP 流量（含用户名密码、消息体）在网络上**明文传输**，抓包即可看到一切。TLS 解决两件事：**加密**（防窃听）+ **身份验证**（防中间人）。

| 链路 | 端口 |
|------|:---:|
| AMQP 明文 | `5672` |
| AMQP over TLS（amqps） | `5671` |
| 管理界面 HTTP | `15672` |
| 管理界面 HTTPS | `15671` |

### 4.2 在 Broker 侧启用 TLS

核心是三个文件：**CA 证书包**、**服务器证书**、**服务器私钥**。用 [tls-gen](https://github.com/rabbitmq/tls-gen) 可一键生成自签证书。

```ini
# rabbitmq.conf
listeners.ssl.default = 5671

ssl_options.cacertfile = /path/to/ca_certificate.pem
ssl_options.certfile   = /path/to/server_certificate.pem
ssl_options.keyfile    = /path/to/server_key.pem

# 对端验证（mTLS 关键）
ssl_options.verify               = verify_peer
ssl_options.fail_if_no_peer_cert = true
```

验证 TLS 是否生效：

```bash
rabbitmq-diagnostics listeners
# 输出里应能看到 protocol: amqp/ssl, port: 5671
```

> **Windows 路径**：配置文件里反斜杠会被当转义符，要么写 `c:\\ca.pem`，要么用正斜杠 `c:/ca.pem`。

### 4.3 mTLS（双向 TLS）

`verify = verify_peer` + `fail_if_no_peer_cert = true` 即 mTLS：服务端验客户端 + 客户端验服务端，最安全。配合 `EXTERNAL` 机制还能用证书身份直接登录：

```bash
rabbitmq-plugins enable rabbitmq_auth_mechanism_ssl
```

```ini
auth_mechanisms.1 = EXTERNAL
auth_mechanisms.2 = PLAIN
ssl_cert_login_from = common_name   # 从证书 CN 取用户名
```

---

## 五、运行方式

### 前置

- **普通模式**（`app.tls.enabled=false`，默认）：只需本机（或可达的）RabbitMQ，开启管理台插件，`localhost:5672` + `admin/admin` + `vhost=/`。
- **TLS 模式**（`app.tls.enabled=true`）：需 **Broker 侧先开启 TLS 监听**（见上 4.2），有合法的服务端证书与 CA。自签证书可用 `tls-gen` 生成。

### 启动

```bash
# 在仓库根目录
mvn -pl ch15-security -am spring-boot:run
```

或在 IDE 里直接运行 `io.github.codecorey.security.SecurityDemoApplication`。

启动后流程：
1. 读取 `app.tls.enabled`，决定走 amqp 还是 amqps；
2. TLS 启用时，若 `app.port` 未改（仍 5672），自动用 amqps 默认端口 **5671**；
3. 按 `app.tls.truststore` 是否配置，走 JVM 默认信任库或自定义信任库；
4. 连接 Broker → 声明队列 `security.tls` → 发一条消息 → 日志打印成功或清晰失败提示。

### 切换 TLS

编辑 `src/main/resources/application.yml`：

```yaml
app:
  tls:
    enabled: true
    # 自签证书时，把 CA 证书导入信任库后指定路径：
    # truststore: /path/to/client.truststore.p12
    # truststore-password: changeit
```

或命令行覆盖：`mvn -pl ch15-security -am spring-boot:run -Dspring-boot.run.arguments="--app.tls.enabled=true"`。

### TLS 失败时的日志（预期）

默认 Docker 镜像未开 TLS，启用 TLS 后会看到：

```
[*] 模式: amqps (TLS)，目标 localhost:5671，用户 admin，vhost /
[tls] 未配置信任库，使用 JVM 默认信任库 + useSslProtocol()
[!] 连接 Broker 失败: (连接/握手失败原因)
    提示：默认 Docker 镜像未开启 TLS 监听。
    请先在 Broker 配置 listeners.ssl.default = 5671 及证书（见 README），
    或把 app.tls.enabled 改回 false 走普通 AMQP 验证连通性。
```

---

## 六、配置项（`application.yml` 的 `app.*`）

| 配置项 | 默认值 | 说明 |
|------|------|------|
| `app.host` / `app.port` | `localhost` / `5672` | Broker 地址端口；启用 TLS 且未改 port 时自动用 5671 |
| `app.username` / `app.password` | `admin` / `admin` | 登录账号 |
| `app.virtual-host` | `/` | vhost |
| `app.queue` | `security.tls` | 演示用队列（durable） |
| `app.tls.enabled` | `false` | 是否走 amqps |
| `app.tls.truststore` | （空） | 自定义信任库路径（JKS/PKCS12）；留空用 JVM 默认信任库 |
| `app.tls.truststore-password` | （空） | 信任库密码 |

---

## 七、代码结构

```
io.github.codecorey.security
├── SecurityDemoApplication     # @SpringBootApplication 入口
├── config
│   └── SecurityProperties       # @ConfigurationProperties("app")：连接参数 + TLS 开关 / 信任库
└── runner
    └── TlsConnectionRunner      # CommandLineRunner：按 app.tls.enabled 选 amqp/amqps，连上发一条消息
```

- TLS 启用路径：`ConnectionFactory.useSslProtocol()`（默认信任库）或 `useSslProtocol(sslContext)`（自定义信任库，`SSLContext` 由 truststore 构造）；锁版本可用 `useSslProtocol("TLSv1.2")`。
- 全程 try/catch：默认 Docker broker 未开 TLS，连不上属预期，打印清晰提示而非抛栈。
- `javax.net.ssl.*` / `java.security.KeyStore` 均为 JDK 内置类，与 Jakarta EE 的 `javax.*`→`jakarta.*` 迁移无关。

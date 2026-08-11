package io.github.codecorey.progmodel.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * 绑定 application.yml 中 {@code rabbitmq.practice.*} 配置（连接参数 + 拓扑命名）。
 *
 * <p>博客原文把连接参数硬编码在示例里（{@code factory.setHost(...)}），这里抽成配置，方便切换 Broker。
 * 默认值对应博客 1.1 节的 Docker 镜像（本机 rabbitmq:3.13-management）；
 * 博客原文常用 {@code 192.168.65.112 + vhost=/mirror}，按需在 application.yml 修改。
 */
@Component
@ConfigurationProperties(prefix = "rabbitmq.practice")
public class RabbitMqProperties {

    /** Broker 地址 */
    private String host = "localhost";
    /** AMQP 端口 */
    private int port = 5672;
    /** 用户名 */
    private String username = "admin";
    /** 密码 */
    private String password = "admin";
    /** 虚拟主机（博客原文常用 /mirror） */
    private String virtualHost = "/";

    /** 主交换机名 */
    private String exchange = "exchange.programming.model";
    /** 备选交换机名（alternate-exchange 兜底） */
    private String alternateExchange = "exchange.programming.model.ae";
    /** 主队列名（演示 basicGet / basicConsume） */
    private String queue = "queue.programming.model";
    /** 备选队列名（观察兜底消息） */
    private String alternateQueue = "queue.programming.model.ae";
    /** 主路由键（主交换机 → 主队列） */
    private String routingKey = "prog.key";
    /** 不可路由键（用于触发 alternate-exchange 兜底） */
    private String unroutableKey = "prog.unroutable";
    /** 主队列类型：quorum（博客 Step 3 Quorum 声明）或 classic */
    private String queueType = "quorum";

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getVirtualHost() {
        return virtualHost;
    }

    public void setVirtualHost(String virtualHost) {
        this.virtualHost = virtualHost;
    }

    public String getExchange() {
        return exchange;
    }

    public void setExchange(String exchange) {
        this.exchange = exchange;
    }

    public String getAlternateExchange() {
        return alternateExchange;
    }

    public void setAlternateExchange(String alternateExchange) {
        this.alternateExchange = alternateExchange;
    }

    public String getQueue() {
        return queue;
    }

    public void setQueue(String queue) {
        this.queue = queue;
    }

    public String getAlternateQueue() {
        return alternateQueue;
    }

    public void setAlternateQueue(String alternateQueue) {
        this.alternateQueue = alternateQueue;
    }

    public String getRoutingKey() {
        return routingKey;
    }

    public void setRoutingKey(String routingKey) {
        this.routingKey = routingKey;
    }

    public String getUnroutableKey() {
        return unroutableKey;
    }

    public void setUnroutableKey(String unroutableKey) {
        this.unroutableKey = unroutableKey;
    }

    public String getQueueType() {
        return queueType;
    }

    public void setQueueType(String queueType) {
        this.queueType = queueType;
    }
}

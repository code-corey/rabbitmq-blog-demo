package io.github.codecorey.dlxdelay.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * 绑定 application.yml 中 {@code rabbitmq.delay.*} 配置。
 *
 * <p>博客第五节没有单独的配置层（连接参数与拓扑名散落在示例代码里）；
 * 这里抽成 {@link ConfigurationProperties}，方便切换 Broker 与拓扑名，
 * 连接参数默认 localhost:5672/admin/admin/vhost=/。
 */
@Component
@ConfigurationProperties(prefix = "rabbitmq.delay")
public class RabbitMqProperties {

    /** Broker 地址，默认 localhost（对应博客 1.1 节本机 Docker 镜像） */
    private String host = "localhost";
    /** AMQP 端口，默认 5672 */
    private int port = 5672;
    /** 用户名，默认 admin */
    private String username = "admin";
    /** 密码，默认 admin */
    private String password = "admin";
    /** vhost，默认 / */
    private String virtualHost = "/";

    /** Producer 投递的交换机 */
    private String delayExchange = "delay.exchange";
    /** 设 TTL + DLX 的延迟队列，无 Consumer，消息在此等待过期 */
    private String delayQueue = "delay.queue";
    /** Producer → delay.exchange 使用的 routing key */
    private String delayRoutingKey = "delay.order";

    /** 死信交换机（DLX），过期消息转发目标 */
    private String processExchange = "process.exchange";
    /** 死信队列，由 Consumer 消费执行关单 */
    private String processQueue = "process.queue";
    /** delay.queue 的 x-dead-letter-routing-key 指向它 */
    private String processRoutingKey = "process.order";

    /**
     * 消息 TTL（毫秒）。博客第五节原文为 1800000（30 分钟，订单关单）；
     * 演示用 5000（5 秒）。
     */
    private long messageTtl = 5000;

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

    public String getDelayExchange() {
        return delayExchange;
    }

    public void setDelayExchange(String delayExchange) {
        this.delayExchange = delayExchange;
    }

    public String getDelayQueue() {
        return delayQueue;
    }

    public void setDelayQueue(String delayQueue) {
        this.delayQueue = delayQueue;
    }

    public String getDelayRoutingKey() {
        return delayRoutingKey;
    }

    public void setDelayRoutingKey(String delayRoutingKey) {
        this.delayRoutingKey = delayRoutingKey;
    }

    public String getProcessExchange() {
        return processExchange;
    }

    public void setProcessExchange(String processExchange) {
        this.processExchange = processExchange;
    }

    public String getProcessQueue() {
        return processQueue;
    }

    public void setProcessQueue(String processQueue) {
        this.processQueue = processQueue;
    }

    public String getProcessRoutingKey() {
        return processRoutingKey;
    }

    public void setProcessRoutingKey(String processRoutingKey) {
        this.processRoutingKey = processRoutingKey;
    }

    public long getMessageTtl() {
        return messageTtl;
    }

    public void setMessageTtl(long messageTtl) {
        this.messageTtl = messageTtl;
    }
}

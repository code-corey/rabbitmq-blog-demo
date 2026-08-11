package io.github.codecorey.sharding.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * 绑定 application.yml 中 {@code rabbitmq.practice.*} 配置。
 *
 * <p>博客第 8 篇把连接参数硬编码在 ShardingProducer / ShardingConsumer 里（192.168.65.112 / admin / /mirror），
 * 这里抽成配置，方便切换 Broker。{@code exchange} 既是 x-modulus-hash 交换机名，也是消费端的伪队列名。
 */
@Component
@ConfigurationProperties(prefix = "rabbitmq.practice")
public class RabbitMqProperties {

    private String host = "localhost";
    private int port = 5672;
    private String username = "admin";
    private String password = "admin";
    private String virtualHost = "/";
    /** x-modulus-hash 交换机名，同时作为消费端的伪队列名。 */
    private String exchange = "sharding_exchange";
    /** 生产者发送的消息条数（博客为 3000）。 */
    private int messageCount = 3000;

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

    public int getMessageCount() {
        return messageCount;
    }

    public void setMessageCount(int messageCount) {
        this.messageCount = messageCount;
    }
}

package io.github.codecorey.queuetypes.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * 绑定 application.yml 中 {@code rabbitmq.practice.*} 配置。
 *
 * <p>博客 4.1 / 4.2 把连接参数硬编码在示例里；这里抽成配置，方便切换 Broker。
 * 默认值对应本机 {@code rabbitmq:3.13-management}；博客原文用 {@code 192.168.65.112} + vhost {@code /mirror}，
 * 改 application.yml 即可。
 */
@Component
@ConfigurationProperties(prefix = "rabbitmq.practice")
public class RabbitMqProperties {

    private String host = "localhost";
    private int port = 5672;
    private String username = "admin";
    private String password = "admin";
    private String virtualHost = "/";

    private String classicQueue = "classic.queue";
    private String quorumQueue = "quorum.queue";
    private String streamQueue = "stream.queue";

    /** Stream 消费起点：first / last / next / 数字偏移量。博客示例取 last。 */
    private String streamOffset = "first";

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

    public String getClassicQueue() {
        return classicQueue;
    }

    public void setClassicQueue(String classicQueue) {
        this.classicQueue = classicQueue;
    }

    public String getQuorumQueue() {
        return quorumQueue;
    }

    public void setQuorumQueue(String quorumQueue) {
        this.quorumQueue = quorumQueue;
    }

    public String getStreamQueue() {
        return streamQueue;
    }

    public void setStreamQueue(String streamQueue) {
        this.streamQueue = streamQueue;
    }

    public String getStreamOffset() {
        return streamOffset;
    }

    public void setStreamOffset(String streamOffset) {
        this.streamOffset = streamOffset;
    }
}

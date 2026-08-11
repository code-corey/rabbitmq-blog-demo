package io.github.codecorey.queueconcepts.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * 绑定 application.yml 中 {@code rabbitmq.practice.*} 连接配置。
 *
 * <p>博客代码把连接参数硬编码在示例里；这里抽成配置，方便切换 Broker
 * （默认 localhost:5672/admin/admin/vhost=/，博客原文常为 192.168.65.112//mirror）。
 */
@Component
@ConfigurationProperties(prefix = "rabbitmq.practice")
public class RabbitMqProperties {

    private String host = "localhost";
    private int port = 5672;
    private String username = "admin";
    private String password = "admin";
    private String virtualHost = "/";

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
}

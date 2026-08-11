package io.github.codecorey.protocols.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * 绑定 application.yml 中 {@code rabbitmq.*} 连接配置。
 *
 * <p>只服务 AMQP 0-9-1 基线 demo；MQTT / STOMP / Stream 的端口与配置见 README。
 */
@Component
@ConfigurationProperties(prefix = "rabbitmq")
public class ProtocolProperties {

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

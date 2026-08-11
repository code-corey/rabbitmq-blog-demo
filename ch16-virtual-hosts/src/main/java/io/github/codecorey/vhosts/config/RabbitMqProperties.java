package io.github.codecorey.vhosts.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * 绑定 application.yml 中 {@code rabbitmq.vhost.*} 配置。
 *
 * <p>博客第五节示例在代码里硬编码连接参数；这里抽成 {@link ConfigurationProperties}，
 * 方便切换 Broker，尤其体现「换 vhost 只需改配置」。
 * 连接参数默认 localhost:5672/admin/admin/vhost=/（默认 vhost 开箱即用）。
 */
@Component
@ConfigurationProperties(prefix = "rabbitmq.vhost")
public class RabbitMqProperties {

    /** Broker 地址，默认 localhost（对应博客本机 Docker 镜像） */
    private String host = "localhost";
    /** AMQP 端口，默认 5672 */
    private int port = 5672;
    /** 用户名，默认 admin */
    private String username = "admin";
    /** 密码，默认 admin */
    private String password = "admin";
    /** vhost，默认 /（开箱即用）；可改成自定义 vhost 如 /mirror（需先在 Broker 建好并授权） */
    private String virtualHost = "/";
    /** 演示用队列名 */
    private String queue = "vhost.demo.queue";

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

    public String getQueue() {
        return queue;
    }

    public void setQueue(String queue) {
        this.queue = queue;
    }
}

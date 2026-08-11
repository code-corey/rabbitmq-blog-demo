package io.github.codecorey.security.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * 安全 demo 配置（绑定 application.yml 的 {@code app.*}）。
 *
 * <p>承载连接参数与 TLS 开关：默认走普通 AMQP（5672）；
 * 把 {@code app.tls.enabled} 置为 {@code true} 后，演示代码改走 amqps（默认端口 5671），
 * 并可选加载自定义信任库（truststore）做服务端证书校验。
 */
@Component
@ConfigurationProperties(prefix = "app")
public class SecurityProperties {

    /** Broker 地址 */
    private String host = "localhost";
    /** Broker 端口（普通 AMQP 默认 5672；启用 TLS 且未显式配置时自动改用 5671） */
    private int port = 5672;
    /** 用户名 */
    private String username = "admin";
    /** 密码 */
    private String password = "admin";
    /** vhost */
    private String virtualHost = "/";
    /** 演示用队列名（durable） */
    private String queue = "security.tls";
    /** TLS 相关配置 */
    private Tls tls = new Tls();

    public static class Tls {

        /** 是否启用 TLS（amqps） */
        private boolean enabled = false;
        /** 信任库路径（JKS/PKCS12）；留空则用 JVM 默认信任库 */
        private String truststore;
        /** 信任库密码 */
        private String truststorePassword;

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }

        public String getTruststore() {
            return truststore;
        }

        public void setTruststore(String truststore) {
            this.truststore = truststore;
        }

        public String getTruststorePassword() {
            return truststorePassword;
        }

        public void setTruststorePassword(String truststorePassword) {
            this.truststorePassword = truststorePassword;
        }
    }

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

    public Tls getTls() {
        return tls;
    }

    public void setTls(Tls tls) {
        this.tls = tls;
    }
}

package io.github.codecorey.security.runner;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.MessageProperties;
import io.github.codecorey.security.config.SecurityProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import java.io.FileInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.security.KeyStore;
import java.time.LocalTime;

/**
 * TLS 连接演示（对应博客《RabbitMQ 安全——认证、授权与 TLS》第五章「TLS / SSL：加密链路」）。
 *
 * <p>根据 {@code app.tls.enabled} 选择链路：
 * <ul>
 *   <li>{@code false}（默认）：普通 AMQP（amqp://，端口默认 5672）；</li>
 *   <li>{@code true}：调用 {@link ConnectionFactory#useSslProtocol()} 走 amqps
 *       （端口默认 5671），可选加载自定义信任库校验服务端证书。</li>
 * </ul>
 *
 * <p>连接逻辑全部用 try/catch 包好：默认 Docker 镜像未开 TLS，握手 / 连接失败属预期，
 * 会在日志里打印清晰提示而非抛栈。实跑 TLS 需 Broker 侧先开启 TLS 监听（见 README）。
 */
@Component
public class TlsConnectionRunner implements CommandLineRunner {

    private static final Logger log = LoggerFactory.getLogger(TlsConnectionRunner.class);

    /** 普通 AMQP 默认端口 */
    private static final int PLAIN_AMQP_PORT = 5672;
    /** amqps（AMQP over TLS）默认端口 */
    private static final int AMQPS_PORT = 5671;

    private final SecurityProperties props;

    public TlsConnectionRunner(SecurityProperties props) {
        this.props = props;
    }

    @Override
    public void run(String... args) {
        SecurityProperties.Tls tls = props.getTls();
        boolean tlsEnabled = tls.isEnabled();

        // 端口：TLS 启用且端口仍是普通 AMQP 默认值时，自动用 amqps 默认端口 5671
        int port = props.getPort();
        if (tlsEnabled && port == PLAIN_AMQP_PORT) {
            port = AMQPS_PORT;
            log.info("[tls] app.port 未显式配置，启用 TLS 后自动使用 amqps 默认端口 {}", AMQPS_PORT);
        }

        log.info("[*] 模式: {}，目标 {}:{}，用户 {}，vhost {}",
                tlsEnabled ? "amqps (TLS)" : "amqp (明文)",
                props.getHost(), port, props.getUsername(), props.getVirtualHost());

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(props.getHost());
        factory.setPort(port);
        factory.setUsername(props.getUsername());
        factory.setPassword(props.getPassword());
        factory.setVirtualHost(props.getVirtualHost());

        if (tlsEnabled) {
            try {
                configureTls(factory, tls);
            } catch (Exception e) {
                log.error("[!] TLS 初始化失败: {}", e.getMessage());
                log.error("    请检查信任库路径 / 密码是否正确（当前: {}）",
                        tls.getTruststore() == null || tls.getTruststore().isBlank()
                                ? "使用 JVM 默认信任库" : tls.getTruststore());
                return;
            }
        }

        String queue = props.getQueue();
        String message = "hello over " + (tlsEnabled ? "amqps" : "amqp") + " @ " + LocalTime.now();

        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {

            channel.queueDeclare(queue, true, false, false, null);
            channel.basicPublish("", queue,
                    MessageProperties.PERSISTENT_TEXT_PLAIN,
                    message.getBytes(StandardCharsets.UTF_8));

            log.info("[x] 已通过 {} 发送一条消息到队列 {}: {}",
                    tlsEnabled ? "amqps" : "amqp", queue, message);
        } catch (Exception e) {
            // 默认 Docker 镜像未开 TLS，握手 / 连接失败属预期
            log.error("[!] 连接 Broker 失败: {}", e.getMessage());
            if (tlsEnabled) {
                log.error("    提示：默认 Docker 镜像未开启 TLS 监听。");
                log.error("    请先在 Broker 配置 listeners.ssl.default = {} 及证书（见 README），", AMQPS_PORT);
                log.error("    或把 app.tls.enabled 改回 false 走普通 AMQP 验证连通性。");
            } else {
                log.error("    提示：请确认 Broker 已启动且 {}:{} 可达、账号密码正确。",
                        props.getHost(), port);
            }
        }
    }

    /**
     * 配置 TLS：
     * <ul>
     *   <li>未配信任库：{@code factory.useSslProtocol()}（JVM 默认信任库）。
     *       如需锁定版本可改用 {@code factory.useSslProtocol("TLSv1.2")}。</li>
     *   <li>配了信任库：加载 JKS/PKCS12 → TrustManagerFactory → SSLContext，
     *       再 {@code factory.useSslProtocol(sslContext)} 校验服务端证书。</li>
     * </ul>
     *
     * <p>注意：{@code javax.net.ssl.*}、{@code java.security.KeyStore} 均为 JDK 内置类，
     * 与 Jakarta EE 的 {@code javax.*}→{@code jakarta.*} 迁移无关。
     */
    private void configureTls(ConnectionFactory factory, SecurityProperties.Tls tls) throws Exception {
        String truststore = tls.getTruststore();
        if (truststore == null || truststore.isBlank()) {
            log.info("[tls] 未配置信任库，使用 JVM 默认信任库 + useSslProtocol()");
            // 简单写法：默认协议；如需锁定版本可用 factory.useSslProtocol("TLSv1.2")
            factory.useSslProtocol();
        } else {
            log.info("[tls] 加载自定义信任库: {}", truststore);
            char[] pass = tls.getTruststorePassword() == null
                    ? new char[0] : tls.getTruststorePassword().toCharArray();

            KeyStore tks = KeyStore.getInstance(KeyStore.getDefaultType());
            try (InputStream in = new FileInputStream(truststore)) {
                tks.load(in, pass);
            }
            TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            tmf.init(tks);

            SSLContext ctx = SSLContext.getInstance("TLSv1.2");
            ctx.init(null, tmf.getTrustManagers(), null);
            factory.useSslProtocol(ctx);
        }
    }
}

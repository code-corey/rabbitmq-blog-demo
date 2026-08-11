package io.github.codecorey.security;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * 第 15 篇配套入口：安全 demo（TLS / amqps 连接示例）。
 *
 * <p>启动后由 {@link io.github.codecorey.security.runner.TlsConnectionRunner} 根据
 * {@code app.tls.enabled} 选择 amqp 或 amqps 链路连上 Broker、发一条消息，
 * 直观演示「链路加密」开关的切换与失败时的清晰提示。
 *
 * <p>实跑 TLS 需 Broker 侧先开启 TLS 监听（见 README）；默认 Docker 镜像未开 TLS，
 * 连不上会在日志里打印清晰提示而非抛栈。
 */
@SpringBootApplication
public class SecurityDemoApplication {

    public static void main(String[] args) {
        SpringApplication.run(SecurityDemoApplication.class, args);
    }
}

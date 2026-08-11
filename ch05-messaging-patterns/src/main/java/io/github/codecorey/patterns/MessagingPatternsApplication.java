package io.github.codecorey.patterns;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * 博客《RabbitMQ 常用消息场景——Work、Pub/Sub、Routing、Topic》示例入口（原生 amqp-client）。
 *
 * <p>启动后由 {@link PatternsDemoRunner} 依次演示六种消息场景 + Publisher Confirms：
 * Hello World / Work Queue / Publish-Subscribe / Routing / Topic / Headers / Confirms。
 */
@SpringBootApplication
public class MessagingPatternsApplication {

    public static void main(String[] args) {
        SpringApplication.run(MessagingPatternsApplication.class, args);
    }
}

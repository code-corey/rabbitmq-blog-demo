package io.github.codecorey.rabbitmqlearning;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * RabbitMQ 练习项目入口（对应博客《RabbitMQ 安装与核心概念》5.1 / 5.2）。
 *
 * <p>默认运行消费者（FirstConsumer）；加 {@code --app.mode=send} 切换为发送一条消息（FirstProducer）。
 */
@SpringBootApplication
public class RabbitmqLearningApplication {

    public static void main(String[] args) {
        SpringApplication.run(RabbitmqLearningApplication.class, args);
    }
}

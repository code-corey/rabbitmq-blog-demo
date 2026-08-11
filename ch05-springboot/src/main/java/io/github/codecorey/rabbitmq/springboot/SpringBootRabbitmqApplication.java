package io.github.codecorey.rabbitmq.springboot;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * 博客《SpringBoot 集成 RabbitMQ》示例入口。
 *
 * <p>启动后自动声明 Exchange/Queue/Binding，发送 3 条消息并由 @RabbitListener 消费（手动 ACK）。
 */
@SpringBootApplication
public class SpringBootRabbitmqApplication {

    public static void main(String[] args) {
        SpringApplication.run(SpringBootRabbitmqApplication.class, args);
    }
}

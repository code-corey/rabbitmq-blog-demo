package io.github.codecorey.progmodel;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * 博客《RabbitMQ 基础编程模型——从连接到消费》示例入口（第 03 篇）。
 *
 * <p>启动后由 {@link io.github.codecorey.progmodel.runner.ProgrammingModelDemoRunner}
 * 自动演示原生 amqp-client 七步编程模型，重点对比 {@code basicGet}（拉）与 {@code basicConsume}（推）
 * 两种消费方式，并补充 Quorum 队列声明与 alternate-exchange 兜底示例。
 */
@SpringBootApplication
public class ProgrammingModelApplication {

    public static void main(String[] args) {
        SpringApplication.run(ProgrammingModelApplication.class, args);
    }
}

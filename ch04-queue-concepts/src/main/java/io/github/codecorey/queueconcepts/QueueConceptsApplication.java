package io.github.codecorey.queueconcepts;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * 博客《RabbitMQ 队列核心概念——命名、顺序、优先级与策略》示例入口（原生 amqp-client）。
 *
 * <p>启动后由 {@link QueueConceptsDemoRunner} 依次演示四个队列核心概念：
 * 优先级队列 / 服务端命名队列 / 临时独占队列 / 声明等价（PRECONDITION_FAILED）。
 */
@SpringBootApplication
public class QueueConceptsApplication {

    public static void main(String[] args) {
        SpringApplication.run(QueueConceptsApplication.class, args);
    }
}

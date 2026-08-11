package io.github.codecorey.rabbitmq.springboot.config;

import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * 对应博客第三节：通过 Bean 声明 Exchange / Queue / Binding，启动时自动在 Broker 创建。
 */
@Configuration
public class RabbitConfig {

    public static final String EXCHANGE = "demo.exchange";
    public static final String QUEUE = "demo.queue";
    public static final String ROUTING_KEY = "demo.key";

    /** Direct 交换机：(name, durable, autoDelete) */
    @Bean
    public DirectExchange demoExchange() {
        return new DirectExchange(EXCHANGE, true, false);
    }

    /** 经典持久队列 */
    @Bean
    public Queue demoQueue() {
        return QueueBuilder.durable(QUEUE).build();
    }

    /** 绑定：demoQueue --(demo.key)--> demoExchange */
    @Bean
    public Binding demoBinding(Queue demoQueue, DirectExchange demoExchange) {
        return BindingBuilder.bind(demoQueue).to(demoExchange).with(ROUTING_KEY);
    }

    /**
     * 对应博客「Quorum 队列示例」：仅声明演示，本例不消费它。
     * 单节点 Broker 也能声明仲裁队列（副本数 = 1）。
     */
    @Bean
    public Queue quorumQueue() {
        return QueueBuilder.durable("quorum.queue")
                .withArgument("x-queue-type", "quorum")
                .build();
    }
}

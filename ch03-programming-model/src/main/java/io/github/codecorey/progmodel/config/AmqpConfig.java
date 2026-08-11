package io.github.codecorey.progmodel.config;

import com.rabbitmq.client.ConnectionFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * 原生 amqp-client 的 {@link ConnectionFactory} Bean（对应博客 Step 1：创建 Connection 的工厂）。
 *
 * <p>连接参数由 {@link RabbitMqProperties} 注入；各 Producer / Consumer 各自调
 * {@code factory.newConnection()} 拿连接、{@code createChannel()} 拿 Channel，演示完整生命周期。
 */
@Configuration
public class AmqpConfig {

    @Bean
    public ConnectionFactory connectionFactory(RabbitMqProperties props) {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(props.getHost());
        factory.setPort(props.getPort());
        factory.setUsername(props.getUsername());
        factory.setPassword(props.getPassword());
        factory.setVirtualHost(props.getVirtualHost());
        return factory;
    }
}

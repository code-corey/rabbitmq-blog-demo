package io.github.codecorey.queuetypes.config;

import com.rabbitmq.client.ConnectionFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * 提供原生 amqp-client 的 {@link ConnectionFactory}（注意：不是 Spring AMQP 的 CachingConnectionFactory）。
 *
 * <p>本模块刻意只引 {@code spring-boot-starter} + 原生 {@code amqp-client}：博客 4.2 明确
 * Spring {@code @RabbitListener} 目前无法直接传 offset 消费 Stream，故全程用原生 Channel。
 */
@Configuration
public class RabbitMqConfig {

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

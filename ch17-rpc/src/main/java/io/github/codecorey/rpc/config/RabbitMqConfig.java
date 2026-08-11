package io.github.codecorey.rpc.config;

import com.rabbitmq.client.ConnectionFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * 用 {@link RabbitMqProperties} 构建一个共享的 amqp-client {@link ConnectionFactory}。
 *
 * <p>本模块只引入 spring-boot-starter + amqp-client（非 spring-boot-starter-amqp），
 * 故此处的 {@link ConnectionFactory} 即原生 {@code com.rabbitmq.client.ConnectionFactory}，
 * 线程安全，可被 server / client 各自复用以 newConnection()。
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

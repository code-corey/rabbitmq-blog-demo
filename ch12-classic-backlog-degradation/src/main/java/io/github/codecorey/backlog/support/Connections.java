package io.github.codecorey.backlog.support;

import com.rabbitmq.client.ConnectionFactory;
import io.github.codecorey.backlog.config.BacklogProperties;

/**
 * 根据 {@link BacklogProperties} 构建 amqp-client {@link ConnectionFactory} 的小工具，
 * 供生产者、消费者、监控共用，避免在各处重复连接参数。
 */
public final class Connections {

    private Connections() {
    }

    public static ConnectionFactory factory(BacklogProperties props) {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(props.getHost());
        factory.setPort(props.getPort());
        factory.setUsername(props.getUsername());
        factory.setPassword(props.getPassword());
        factory.setVirtualHost(props.getVirtualHost());
        return factory;
    }
}

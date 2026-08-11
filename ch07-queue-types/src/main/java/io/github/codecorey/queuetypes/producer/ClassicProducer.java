package io.github.codecorey.queuetypes.producer;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.MessageProperties;
import io.github.codecorey.queuetypes.config.RabbitMqProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.TimeoutException;

/**
 * Classic 经典队列生产者（对比基线）。
 *
 * <p>queueDeclare 不带 {@code x-queue-type} 即默认 Classic：传统 FIFO，消息取走即删。
 * 与 Quorum / Stream 形成对比——后两者必须显式指定 {@code x-queue-type}。
 */
@Component
public class ClassicProducer {

    private static final Logger log = LoggerFactory.getLogger(ClassicProducer.class);

    private final ConnectionFactory factory;
    private final RabbitMqProperties props;

    public ClassicProducer(ConnectionFactory factory, RabbitMqProperties props) {
        this.factory = factory;
        this.props = props;
    }

    public void send(List<String> messages) throws IOException, TimeoutException {
        String queue = props.getClassicQueue();
        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {

            // Classic：durable=true、autoDelete=false，arguments=null（默认类型）
            channel.queueDeclare(queue, true, false, false, null);

            for (String message : messages) {
                channel.basicPublish("", queue,
                        MessageProperties.PERSISTENT_TEXT_PLAIN,
                        message.getBytes(StandardCharsets.UTF_8));
                log.info("[classic][已发送] {}", message);
            }
        }
    }
}

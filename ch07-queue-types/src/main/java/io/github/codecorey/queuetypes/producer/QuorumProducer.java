package io.github.codecorey.queuetypes.producer;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.MessageProperties;
import io.github.codecorey.queuetypes.DeclareArguments;
import io.github.codecorey.queuetypes.config.RabbitMqProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.TimeoutException;

/**
 * Quorum 仲裁队列生产者（博客 4.1）。
 *
 * <p>声明参数 {@code x-queue-type=quorum}；{@code durable=true}、{@code exclusive=false} 为强制要求，
 * 否则 Broker 报错。Producer 与 Consumer 用 {@link DeclareArguments#quorumArgs()} 保证声明一致。
 */
@Component
public class QuorumProducer {

    private static final Logger log = LoggerFactory.getLogger(QuorumProducer.class);

    private final ConnectionFactory factory;
    private final RabbitMqProperties props;

    public QuorumProducer(ConnectionFactory factory, RabbitMqProperties props) {
        this.factory = factory;
        this.props = props;
    }

    public void send(List<String> messages) throws IOException, TimeoutException {
        String queue = props.getQuorumQueue();
        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {

            // durable=true、exclusive=false 为 Quorum 强制；arguments 带 x-queue-type=quorum
            channel.queueDeclare(queue, true, false, false, DeclareArguments.quorumArgs());

            for (String message : messages) {
                channel.basicPublish("", queue,
                        MessageProperties.PERSISTENT_TEXT_PLAIN,
                        message.getBytes(StandardCharsets.UTF_8));
                log.info("[quorum][已发送] {}", message);
            }
        }
    }
}

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
 * Stream 流式队列生产者（博客 4.2）。
 *
 * <p>声明参数 {@code x-queue-type=stream} + {@code x-max-length-bytes}（博客 20_000_000_000L）
 * + {@code x-stream-max-segment-size-bytes}（100_000_000）。消息以 append-only 日志持久化，
 * 不会被消费即删，可重复回放。
 */
@Component
public class StreamProducer {

    private static final Logger log = LoggerFactory.getLogger(StreamProducer.class);

    private final ConnectionFactory factory;
    private final RabbitMqProperties props;

    public StreamProducer(ConnectionFactory factory, RabbitMqProperties props) {
        this.factory = factory;
        this.props = props;
    }

    public void send(List<String> messages) throws IOException, TimeoutException {
        String queue = props.getStreamQueue();
        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {

            // Stream：durable=true、exclusive=false；arguments 见 DeclareArguments.streamArgs()
            channel.queueDeclare(queue, true, false, false, DeclareArguments.streamArgs());

            for (String message : messages) {
                channel.basicPublish("", queue,
                        MessageProperties.PERSISTENT_TEXT_PLAIN,
                        message.getBytes(StandardCharsets.UTF_8));
                log.info("[stream][已发送] {}", message);
            }
        }
    }
}

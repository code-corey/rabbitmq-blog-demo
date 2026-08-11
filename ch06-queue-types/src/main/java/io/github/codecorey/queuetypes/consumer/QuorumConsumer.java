package io.github.codecorey.queuetypes.consumer;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import io.github.codecorey.queuetypes.DeclareArguments;
import io.github.codecorey.queuetypes.config.RabbitMqProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeoutException;

/**
 * Quorum 仲裁队列消费者（博客 4.1）。
 *
 * <p>声明 {@code x-queue-type=quorum}（durable=true、exclusive=false 强制）→ {@code basicQos(1)} →
 * {@code basicConsume} 手动 ACK。声明参数与 {@link io.github.codecorey.queuetypes.producer.QuorumProducer}
 * 一致，均取自 {@link DeclareArguments#quorumArgs()}。
 */
@Component
public class QuorumConsumer {

    private static final Logger log = LoggerFactory.getLogger(QuorumConsumer.class);

    private final ConnectionFactory factory;
    private final RabbitMqProperties props;

    public QuorumConsumer(ConnectionFactory factory, RabbitMqProperties props) {
        this.factory = factory;
        this.props = props;
    }

    public void start() throws IOException, TimeoutException {
        String queue = props.getQuorumQueue();
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        // durable=true、exclusive=false 强制；arguments 与 Producer 一致
        channel.queueDeclare(queue, true, false, false, DeclareArguments.quorumArgs());
        channel.basicQos(1);

        log.info("[quorum] 等待消息于队列 {}", queue);

        Consumer consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope,
                                       AMQP.BasicProperties properties, byte[] body)
                    throws IOException {
                log.info("[quorum][收到] routingKey={} deliveryTag={} content={}",
                        envelope.getRoutingKey(), envelope.getDeliveryTag(),
                        new String(body, StandardCharsets.UTF_8));
                channel.basicAck(envelope.getDeliveryTag(), false);
            }
        };
        channel.basicConsume(queue, false, consumer);
    }
}

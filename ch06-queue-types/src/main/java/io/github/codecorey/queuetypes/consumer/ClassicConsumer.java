package io.github.codecorey.queuetypes.consumer;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import io.github.codecorey.queuetypes.config.RabbitMqProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeoutException;

/**
 * Classic 经典队列消费者（对比基线，写法同 ch02）。
 *
 * <p>声明不带 {@code x-queue-type}（默认 Classic）→ {@code basicQos(1)} → {@code basicConsume} 手动 ACK。
 * Connection / Channel 不主动关闭：amqp-client 的 I/O 线程会持有连接，{@code spring.main.keep-alive}
 * 保活进程，异步持续消费。
 */
@Component
public class ClassicConsumer {

    private static final Logger log = LoggerFactory.getLogger(ClassicConsumer.class);

    private final ConnectionFactory factory;
    private final RabbitMqProperties props;

    public ClassicConsumer(ConnectionFactory factory, RabbitMqProperties props) {
        this.factory = factory;
        this.props = props;
    }

    public void start() throws IOException, TimeoutException {
        String queue = props.getClassicQueue();
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        // Classic 默认类型：arguments=null
        channel.queueDeclare(queue, true, false, false, null);
        channel.basicQos(1);

        log.info("[classic] 等待消息于队列 {}", queue);

        Consumer consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope,
                                       AMQP.BasicProperties properties, byte[] body)
                    throws IOException {
                log.info("[classic][收到] routingKey={} deliveryTag={} content={}",
                        envelope.getRoutingKey(), envelope.getDeliveryTag(),
                        new String(body, StandardCharsets.UTF_8));
                channel.basicAck(envelope.getDeliveryTag(), false);
            }
        };
        channel.basicConsume(queue, false, consumer);
    }
}

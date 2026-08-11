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
import java.util.Map;
import java.util.concurrent.TimeoutException;

/**
 * Stream 流式队列消费者（博客 4.2 三步）。
 *
 * <p>博客明确 Spring {@code @RabbitListener} 目前无法直接传 offset 消费 Stream，原生 amqp-client 正合适。
 * 三步：
 * <ol>
 *   <li>{@code basicQos} 必须设置（Stream 的 prefetch 为读取窗口 / 信用量）</li>
 *   <li>正确声明 Stream 参数（{@code x-queue-type=stream} 等）</li>
 *   <li>消费时指定 {@code x-stream-offset}（first / last / next / 数字偏移量）</li>
 * </ol>
 *
 * <p>{@code handleDelivery} 中 {@code basicAck} 用于 Stream 的信用流控，Broker 据此继续推送后续 chunk。
 */
@Component
public class StreamConsumer {

    private static final Logger log = LoggerFactory.getLogger(StreamConsumer.class);

    private final ConnectionFactory factory;
    private final RabbitMqProperties props;

    public StreamConsumer(ConnectionFactory factory, RabbitMqProperties props) {
        this.factory = factory;
        this.props = props;
    }

    public void start() throws IOException, TimeoutException {
        String queue = props.getStreamQueue();
        String offset = props.getStreamOffset();
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        // ② 声明 Stream 参数（durable=true、exclusive=false）
        channel.queueDeclare(queue, true, false, false, DeclareArguments.streamArgs());

        // ① basicQos 必须设置：Stream 下 prefetch 是读取窗口大小
        channel.basicQos(100);

        // ③ 消费参数携带 x-stream-offset
        Map<String, Object> consumeParam = DeclareArguments.streamConsumeArgs(offset);

        log.info("[stream] 等待消息于队列 {}，x-stream-offset={}", queue, offset);

        Consumer consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope,
                                       AMQP.BasicProperties properties, byte[] body)
                    throws IOException {
                // Stream 的 deliveryTag 即对应偏移量
                log.info("[stream][收到] offset={} content={}",
                        envelope.getDeliveryTag(),
                        new String(body, StandardCharsets.UTF_8));
                // 信用流控：ACK 后 Broker 继续推送下一个 chunk
                channel.basicAck(envelope.getDeliveryTag(), false);
            }
        };
        // autoAck=false；把 consumeParam（含 x-stream-offset）随 basicConsume 传入
        channel.basicConsume(queue, false, consumeParam, consumer);
    }
}

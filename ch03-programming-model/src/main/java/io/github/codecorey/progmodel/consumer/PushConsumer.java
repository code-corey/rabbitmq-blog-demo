package io.github.codecorey.progmodel.consumer;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import io.github.codecorey.progmodel.config.RabbitMqProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * 【推模式 basicConsume】对应博客 Step 6 Push + 第四节《消息监听与回调扩展》：服务端把消息推给客户端，
 * 收到后手动 {@code basicAck} 确认。
 *
 * <p>使用 {@code basicConsume} 的多回调重载（逐字复刻博客第四节完整示例）：
 * <pre>
 * channel.basicConsume(queue,
 *     DeliverCallback          —— 收到消息，
 *     CancelCallback           —— 队列被删等取消，
 *     ConsumerShutdownSignalCallback —— 消费者 shutdown
 * );
 * </pre>
 * 与 {@link PullConsumer}（拉模式）相比：basicConsume 注册后由服务端主动推送，实时性更好，是推荐用法。
 * 假设拓扑已由 {@link io.github.codecorey.progmodel.producer.DemoProducer} 声明。
 */
@Component
public class PushConsumer {

    private static final Logger log = LoggerFactory.getLogger(PushConsumer.class);

    private final ConnectionFactory factory;
    private final RabbitMqProperties props;

    private Connection connection;
    private Channel channel;
    private String consumerTag;

    public PushConsumer(ConnectionFactory factory, RabbitMqProperties props) {
        this.factory = factory;
        this.props = props;
    }

    /** Step 1：建连拿 Channel，并设置一次只投一条（对应 basicQos(1)）。 */
    public void open() throws IOException, TimeoutException {
        connection = factory.newConnection();
        channel = connection.createChannel();
        channel.basicQos(1);
    }

    /**
     * 【推模式 basicConsume】注册消费者。autoAck=false（手动 ack）。
     *
     * <p>注意：{@code handleDelivery} 必须 {@code throws IOException}（与博客、ch02 示例一致）。
     */
    public void start(String queue) throws IOException {
        consumerTag = channel.basicConsume(queue, false,
                // DeliverCallback：收到消息
                (tag, message) -> {
                    long deliveryTag = message.getEnvelope().getDeliveryTag();
                    String correlationId = message.getProperties().getCorrelationId();
                    String body = new String(message.getBody());
                    log.info("【推模式 basicConsume】收到: body={}, deliveryTag={}, correlationId={}",
                            body, deliveryTag, correlationId);
                    // 手动确认（multiple=false：仅确认本条）
                    channel.basicAck(deliveryTag, false);
                },
                // CancelCallback：队列被删等导致取消
                tag -> log.info("【推模式 basicConsume】取消: consumerTag={}", tag),
                // ConsumerShutdownSignalCallback：消费者 shutdown
                (tag, sig) -> log.info("【推模式 basicConsume】shutdown: consumerTag={}", tag)
        );
        log.info("【推模式 basicConsume】已注册消费者 consumerTag={}，等待服务端推送...", consumerTag);
    }

    /** 取消注册的消费者（演示结束后调用，便于优雅退出）。 */
    public void stop() throws IOException {
        if (consumerTag != null) {
            channel.basicCancel(consumerTag);
            log.info("【推模式 basicConsume】已取消消费者 consumerTag={}", consumerTag);
            consumerTag = null;
        }
    }

    public void close() throws IOException, TimeoutException {
        if (channel != null && channel.isOpen()) {
            channel.close();
        }
        if (connection != null && connection.isOpen()) {
            connection.close();
        }
    }
}

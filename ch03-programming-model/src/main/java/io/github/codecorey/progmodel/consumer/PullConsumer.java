package io.github.codecorey.progmodel.consumer;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.GetResponse;
import io.github.codecorey.progmodel.config.RabbitMqProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeoutException;

/**
 * 【拉模式 basicGet】对应博客 Step 6 Pull：客户端主动调用 {@code channel.basicGet(queue, autoAck)} 拉取消息，
 * 拿到后手动 {@code basicAck} 确认。
 *
 * <p>与 {@link PushConsumer}（推模式）形成对比：
 * <ul>
 *   <li>basicGet 是"问一次、取一条"，没有消息时返回 {@code null}，完全由客户端节奏控制。</li>
 *   <li>autoAck=false（手动 ack）——与博客保持一致，未 Ack 的消息会被重复投递。</li>
 * </ul>
 * 假设拓扑（Exchange/Queue/Binding）已由 {@link io.github.codecorey.progmodel.producer.DemoProducer} 声明。
 */
@Component
public class PullConsumer {

    private static final Logger log = LoggerFactory.getLogger(PullConsumer.class);

    private final ConnectionFactory factory;
    private final RabbitMqProperties props;

    private Connection connection;
    private Channel channel;

    public PullConsumer(ConnectionFactory factory, RabbitMqProperties props) {
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
     * 【拉模式 basicGet】从指定队列拉取至多 {@code max} 条消息，逐条手动 ack。
     *
     * @return 实际拉取到的条数（队列空时为 0）
     */
    public int pull(String queue, int max) throws IOException {
        int got = 0;
        for (int i = 0; i < max; i++) {
            // autoAck=false：投递不自动确认，需手动 basicAck
            GetResponse response = channel.basicGet(queue, false);
            if (response == null) {
                // 队列暂无消息
                break;
            }
            long deliveryTag = response.getEnvelope().getDeliveryTag();
            String correlationId = response.getProps().getCorrelationId();
            String body = new String(response.getBody(), StandardCharsets.UTF_8);
            log.info("【拉模式 basicGet】收到: body={}, deliveryTag={}, correlationId={}", body, deliveryTag, correlationId);
            // 手动确认（multiple=false：仅确认本条）
            channel.basicAck(deliveryTag, false);
            got++;
        }
        return got;
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

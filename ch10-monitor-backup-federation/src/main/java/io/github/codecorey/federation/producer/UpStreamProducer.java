package io.github.codecorey.federation.producer;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.MessageProperties;
import io.github.codecorey.federation.config.FederationProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.time.LocalTime;

/**
 * 向<b>上游</b> Broker 的 {@code fed_exchange} 发送若干消息（联邦场景的消息源）。
 *
 * <p>博客 3.5 测试节："在上游（193）的 {@code fed_exchange} 发消息，下游本地 {@code fed_queue}
 * 的消费者应能收到"。本类扮演该上游发送方：连 upstream，声明同名 direct 交换机，
 * 按 routingKey 发 N 条持久化消息；经联邦链路同步后由
 * {@link io.github.codecorey.federation.consumer.DownStreamConsumer} 在下游消费。
 *
 * <p>{@link Order}(2)：等下游消费者就绪后再发；发送完关闭本连接
 * （下游消费者连接仍由 {@code spring.main.keep-alive} 保活）。
 */
@Component
@Order(2)
public class UpStreamProducer implements CommandLineRunner {

    private static final Logger log = LoggerFactory.getLogger(UpStreamProducer.class);

    private final FederationProperties props;

    public UpStreamProducer(FederationProperties props) {
        this.props = props;
    }

    @Override
    public void run(String... args) throws Exception {
        FederationProperties.Broker upstream = props.getUpstream();

        // 留给下游消费者订阅与联邦链路就绪
        if (props.getProducerDelayMs() > 0) {
            Thread.sleep(props.getProducerDelayMs());
        }

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(upstream.getHost());
        factory.setPort(upstream.getPort());
        factory.setUsername(upstream.getUsername());
        factory.setPassword(upstream.getPassword());
        factory.setVirtualHost(upstream.getVirtualHost());

        String exchange = props.getExchange();
        String routingKey = props.getRoutingKey();

        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {

            // 上游声明同名交换机（联邦上游不存在则自动创建，显式声明幂等且更直观）
            channel.exchangeDeclare(exchange, "direct");

            int count = props.getProducerCount();
            for (int i = 1; i <= count; i++) {
                String message = "federation msg #" + i + " @ " + LocalTime.now();
                channel.basicPublish(exchange, routingKey,
                        MessageProperties.PERSISTENT_TEXT_PLAIN,
                        message.getBytes(StandardCharsets.UTF_8));
                log.info("[x] 已发到上游 {}:{} 的 {}: {}", factory.getHost(), factory.getPort(),
                        exchange, message);
                Thread.sleep(500);
            }
        }

        // 本连接随 try-with-resources 关闭；下游消费者连接仍保活，进程不退出
    }
}

package io.github.codecorey.federation.consumer;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import io.github.codecorey.federation.config.FederationProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * 对应博客《RabbitMQ 监控、备份与联邦同步》3.3 节——DownStreamConsumer（下游消费者）。
 *
 * <p>连接<b>下游</b> Broker，声明本地 Exchange / Queue / Binding，再 {@code basicConsume} 监听。
 * 联邦链路由<b>下游主动</b>在管理控制台配置 upstream URI（{@code amqp://admin:admin@host/vhost}）
 * 并加 Federation Policy 后建立；上游 {@code fed_exchange} 的消息同步到下游，本消费者即可在本地队列收到。
 *
 * <p>与博客原文的差异：连接参数抽到 application.yml（{@link FederationProperties}）；
 * 用 SLF4J 替代 {@code System.out}；{@code basicConsume} 改用 {@link DefaultConsumer} +
 * {@code handleDelivery}（保留博客 {@code autoAck=true} 语义）。
 *
 * <p>{@link Order}(1)：先就绪订阅，{@link io.github.codecorey.federation.producer.UpStreamProducer} 随后发送。
 */
@Component
@Order(1)
public class DownStreamConsumer implements CommandLineRunner {

    private static final Logger log = LoggerFactory.getLogger(DownStreamConsumer.class);

    private final FederationProperties props;

    public DownStreamConsumer(FederationProperties props) {
        this.props = props;
    }

    @Override
    public void run(String... args) throws Exception {
        FederationProperties.Broker downstream = props.getDownstream();

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(downstream.getHost());
        factory.setPort(downstream.getPort());
        factory.setUsername(downstream.getUsername());
        factory.setPassword(downstream.getPassword());
        factory.setVirtualHost(downstream.getVirtualHost());

        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        String exchange = props.getExchange();
        String queue = props.getQueue();
        String routingKey = props.getRoutingKey();

        // 博客 3.3：下游先声明本地 Exchange 与 Queue，再绑定
        channel.exchangeDeclare(exchange, "direct");
        channel.queueDeclare(queue, true, false, false, null);
        channel.queueBind(queue, exchange, routingKey);

        log.info("[*] 下游 {}:{} 等待联邦消息: exchange={}, queue={}, routingKey={}",
                factory.getHost(), factory.getPort(), exchange, queue, routingKey);

        Consumer myconsumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope,
                                       AMQP.BasicProperties properties, byte[] body)
                    throws IOException {
                log.info("routingKey > {}", envelope.getRoutingKey());
                log.info("content: {}", new String(body, StandardCharsets.UTF_8));
            }
        };

        // autoAck=true（与博客一致）；连接不关闭，由 spring.main.keep-alive 保活持续消费
        channel.basicConsume(queue, true, myconsumer);
    }
}

package io.github.codecorey.rabbitmqlearning.consumer;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import io.github.codecorey.rabbitmqlearning.config.RabbitMqProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * 对应博客 5.2《消费者示例》——FirstConsumer。
 *
 * <p>为在 Spring Boot 中可运行，连接参数由 application.yml 注入（{@link RabbitMqProperties}），
 * 其余消费逻辑与博客一致：建立连接 → 声明队列 → {@code basicQos(1)} → {@code basicConsume} + 手动 ACK。
 *
 * <p>默认运行（app.mode 缺省或 = consumer）。
 */
@Component
@ConditionalOnProperty(name = "app.mode", havingValue = "consumer", matchIfMissing = true)
public class FirstConsumer implements CommandLineRunner {

    private static final Logger log = LoggerFactory.getLogger(FirstConsumer.class);

    private final RabbitMqProperties props;

    public FirstConsumer(RabbitMqProperties props) {
        this.props = props;
    }

    @Override
    public void run(String... args) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(props.getHost());
        factory.setPort(props.getPort());
        factory.setUsername(props.getUsername());
        factory.setPassword(props.getPassword());
        factory.setVirtualHost(props.getVirtualHost());

        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        String queue = props.getQueue();
        // 队列名, durable, exclusive, autoDelete, arguments —— 五个参数详解见博客 5.2
        channel.queueDeclare(queue, true, false, false, null);
        channel.basicQos(1);

        log.info("[*] 等待消息于队列 {}，Ctrl+C 退出", queue);

        Consumer myconsumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope,
                                       AMQP.BasicProperties properties, byte[] body)
                    throws IOException {
                log.info("routingKey > {}", envelope.getRoutingKey());
                log.info("deliveryTag > {}", envelope.getDeliveryTag());
                log.info("content: {}", new String(body, StandardCharsets.UTF_8));
                channel.basicAck(envelope.getDeliveryTag(), false);
            }
        };

        channel.basicConsume(queue, myconsumer);
        // run() 返回后，进程由 spring.main.keep-alive 保活，持续消费
    }
}

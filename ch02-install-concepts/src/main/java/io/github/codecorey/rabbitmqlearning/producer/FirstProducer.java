package io.github.codecorey.rabbitmqlearning.producer;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.MessageProperties;
import io.github.codecorey.rabbitmqlearning.config.RabbitMqProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.time.LocalTime;

/**
 * 配套生产者：向 {@link io.github.codecorey.rabbitmqlearning.consumer.FirstConsumer}
 * 监听的队列发送一条持久化消息（对应博客 3.1 的 {@code MessageProperties.PERSISTENT_TEXT_PLAIN}）。
 *
 * <p>运行方式：{@code --app.mode=send}；发送完毕关闭上下文、进程退出。
 */
@Component
@ConditionalOnProperty(name = "app.mode", havingValue = "send")
public class FirstProducer implements CommandLineRunner {

    private static final Logger log = LoggerFactory.getLogger(FirstProducer.class);

    private final RabbitMqProperties props;
    private final ConfigurableApplicationContext ctx;

    public FirstProducer(RabbitMqProperties props, ConfigurableApplicationContext ctx) {
        this.props = props;
        this.ctx = ctx;
    }

    @Override
    public void run(String... args) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(props.getHost());
        factory.setPort(props.getPort());
        factory.setUsername(props.getUsername());
        factory.setPassword(props.getPassword());
        factory.setVirtualHost(props.getVirtualHost());

        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {

            String queue = props.getQueue();
            channel.queueDeclare(queue, true, false, false, null);

            String message = "hello rabbitmq @ " + LocalTime.now();
            // 默认交换机 ""，routingKey = 队列名（直接投到该队列）；持久化文本 = delivery_mode=2，详见博客 3.1
            channel.basicPublish("", queue,
                    MessageProperties.PERSISTENT_TEXT_PLAIN,
                    message.getBytes(StandardCharsets.UTF_8));

            log.info("[x] 已发送: {}", message);
        }

        // 发送完关闭上下文，进程退出
        ctx.close();
    }
}

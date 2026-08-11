package io.github.codecorey.sharding.producer;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import io.github.codecorey.sharding.config.RabbitMqProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;

/**
 * 对应博客《消息分片存储插件 Sharding》ShardingProducer。
 *
 * <p>声明一个 {@code x-modulus-hash} 类型的交换机（由 rabbitmq_sharding 插件提供），向其发送若干条消息。
 * {@code x-modulus-hash} <b>忽略 routingKey 的语义</b>，以轮询方式把消息平均分配到绑定的所有分片 Queue。
 *
 * <p>运行方式：{@code --app.mode=send}；发送完毕关闭上下文、进程退出。
 */
@Component
@ConditionalOnProperty(name = "app.mode", havingValue = "send")
public class ShardingProducer implements CommandLineRunner {

    private static final Logger log = LoggerFactory.getLogger(ShardingProducer.class);

    /** x-modulus-hash 交换机类型，安装 rabbitmq_sharding 插件后才可用。 */
    private static final String EXCHANGE_TYPE = "x-modulus-hash";

    private final RabbitMqProperties props;
    private final ConfigurableApplicationContext ctx;

    public ShardingProducer(RabbitMqProperties props, ConfigurableApplicationContext ctx) {
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

        String exchange = props.getExchange();
        int messageCount = props.getMessageCount();

        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {

            channel.exchangeDeclare(exchange, EXCHANGE_TYPE);

            for (int i = 0; i < messageCount; i++) {
                String message = "Sharding message " + i;
                // routingKey 传序号字符串即可：x-modulus-hash 会忽略其语义，仅做轮询
                channel.basicPublish(exchange, String.valueOf(i), null,
                        message.getBytes(StandardCharsets.UTF_8));
            }

            log.info("[x] 已向 x-modulus-hash 交换机 [{}] 发送 {} 条消息", exchange, messageCount);
        }

        // 发送完关闭上下文，进程退出
        ctx.close();
    }
}

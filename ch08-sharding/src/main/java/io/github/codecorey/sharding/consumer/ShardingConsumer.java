package io.github.codecorey.sharding.consumer;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import io.github.codecorey.sharding.config.RabbitMqProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * 对应博客《消息分片存储插件 Sharding》ShardingConsumer。
 *
 * <p>分片 Queue 名字有规律，但<b>不应</b>逐个声明 Consumer——那样拿到的是零散分片，不符合「逻辑上一整队列」的语义。
 * Sharding 提供 <b>伪队列</b>：声明一个与交换机<b>同名</b>的 Queue，像普通队列一样 {@code basicConsume}。
 * 该 Queue 物理上并不存在，插件在内部把消费请求路由到连接数最少的分片 Queue。
 *
 * <p>分片数为 N 时，对同一伪队列调用 N 次 {@code basicConsume}，即可分散消费各分片 Queue。
 * 使用手动 ACK（autoAck=false），在 handleDelivery 中 {@code basicAck}，否则未确认消息会被持续重投。
 *
 * <p>默认运行（app.mode 缺省或 = consumer）。
 */
@Component
@ConditionalOnProperty(name = "app.mode", havingValue = "consumer", matchIfMissing = true)
public class ShardingConsumer implements CommandLineRunner {

    private static final Logger log = LoggerFactory.getLogger(ShardingConsumer.class);

    private final RabbitMqProperties props;

    /** 分片数：决定对伪队列 basicConsume 的次数，应与 Broker 端 sharding 策略的总分片数一致。 */
    @Value("${app.shards:3}")
    private int shards;

    public ShardingConsumer(RabbitMqProperties props) {
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

        // 伪队列名 = 交换机名（该 Queue 物理上并不存在，由 Sharding 插件内部路由到分片）
        String queueName = props.getExchange();
        channel.queueDeclare(queueName, false, false, false, null);

        Consumer myconsumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope,
                                       AMQP.BasicProperties properties, byte[] body)
                    throws IOException {
                log.info("routingKey > {}", envelope.getRoutingKey());
                log.info("content: {}", new String(body, StandardCharsets.UTF_8));
                // 手动 ACK，否则未确认消息会被持续重投（见博客「注意事项」表）
                channel.basicAck(envelope.getDeliveryTag(), false);
            }
        };

        // 分片数为 N 时，对同一伪队列 basicConsume N 次，分散消费各分片 Queue
        // 插件原理：每次 basicConsume(伪队列名) 会绑定到当前连接数最少的分片 Queue
        for (int i = 0; i < shards; i++) {
            String consumerTag = channel.basicConsume(queueName, false, myconsumer);
            log.info("[*] 已注册第 {}/{} 个分片消费者，consumerTag={}", i + 1, shards, consumerTag);
        }

        log.info("[*] 等待消息于伪队列 [{}]（共 {} 个分片消费者），Ctrl+C 退出", queueName, shards);
        // run() 返回后，进程由 spring.main.keep-alive 保活，持续消费
    }
}

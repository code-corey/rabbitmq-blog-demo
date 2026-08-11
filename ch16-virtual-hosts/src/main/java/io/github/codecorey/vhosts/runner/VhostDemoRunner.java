package io.github.codecorey.vhosts.runner;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.MessageProperties;
import io.github.codecorey.vhosts.config.RabbitMqProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.LocalTime;

/**
 * 对应博客《Virtual Hosts——隔离、权限与配额》第五节——连接时指定 vhost。
 *
 * <p>核心演示：通过 {@link ConnectionFactory#setVirtualHost(String)} 连接到配置指定的 vhost，
 * 声明一个队列，收发一条消息，并打印当前 vhost。
 *
 * <p>换 vhost 只需改 application.yml 的 {@code rabbitmq.vhost.virtual-host}，代码一行不动——
 * 这正是 vhost 作为「逻辑隔离单元 + 配置切换点」的价值。
 *
 * <p>启动后一条龙：连接（指定 vhost）→ 声明队列 → 注册 Consumer → 发一条消息。
 * Consumer 收到后打印所在 vhost 与消息内容，手动 ACK。
 */
@Component
public class VhostDemoRunner implements CommandLineRunner {

    private static final Logger log = LoggerFactory.getLogger(VhostDemoRunner.class);

    private final RabbitMqProperties props;

    public VhostDemoRunner(RabbitMqProperties props) {
        this.props = props;
    }

    @Override
    public void run(String... args) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(props.getHost());
        factory.setPort(props.getPort());
        factory.setUsername(props.getUsername());
        factory.setPassword(props.getPassword());
        // 关键这一行：连接到配置指定的 vhost（博客第五节）
        factory.setVirtualHost(props.getVirtualHost());

        // 打印当前 vhost —— 换 vhost 只需改配置，无需改代码
        log.info("=== 当前 virtual-host = [{}] ===", props.getVirtualHost());

        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        String queue = props.getQueue();
        // durable=true；不同 vhost 下同名队列互不相干（vhost 是资源隔离单元，博客第一节）
        channel.queueDeclare(queue, true, false, false, null);
        channel.basicQos(1);

        log.info("[*] 队列 {} 已在 vhost [{}] 中声明，等待消息", queue, props.getVirtualHost());

        // 注册 Consumer：收到消息后打印所在 vhost + 内容，手动 ACK
        channel.basicConsume(queue, false, new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope,
                                       AMQP.BasicProperties properties, byte[] body)
                    throws IOException {
                String content = new String(body, StandardCharsets.UTF_8);
                log.info("[x] 在 vhost [{}] 收到: {}（接收时间={}）",
                        props.getVirtualHost(), content, LocalTime.now());
                channel.basicAck(envelope.getDeliveryTag(), false);
            }
        });

        // 发一条消息（默认交换机 ""，routingKey = 队列名，直接投到该队列）
        String message = "hello from vhost [" + props.getVirtualHost() + "] @ " + LocalTime.now();
        channel.basicPublish("", queue,
                MessageProperties.PERSISTENT_TEXT_PLAIN,
                message.getBytes(StandardCharsets.UTF_8));
        log.info("[→] 已发送到 vhost [{}] 的队列 {}: {}", props.getVirtualHost(), queue, message);

        // run() 返回后，进程由 spring.main.keep-alive 保活，Consumer 持续消费
    }
}

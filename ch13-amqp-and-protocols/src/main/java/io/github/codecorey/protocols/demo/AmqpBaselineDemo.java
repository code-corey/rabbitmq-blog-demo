package io.github.codecorey.protocols.demo;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.GetResponse;
import com.rabbitmq.client.MessageProperties;
import io.github.codecorey.protocols.config.ProtocolProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.time.LocalTime;
import java.util.Map;

/**
 * AMQP 0-9-1 基线 demo（对应博客《AMQP 1.0 与多协议》一、二节）。
 *
 * <p>用原生 amqp-client 连本机 Broker（5672，AMQP 0-9-1——RabbitMQ 的「母语」与默认协议）：
 * 打印协议版本信息 + Broker 信息，收发一条消息，作为多协议篇的「默认协议」基线。
 * 同一个 5672 端口同时承载 0-9-1 与 1.0——客户端在连接初始用协议头声明版本，Broker 据此走对应协议栈。
 *
 * <p>MQTT / STOMP / Stream 的端口、配置与命令行测试见 README（不写成 Java demo，避免引入重依赖）。
 */
@Component
public class AmqpBaselineDemo implements CommandLineRunner {

    private static final Logger log = LoggerFactory.getLogger(AmqpBaselineDemo.class);

    private final ProtocolProperties props;
    private final ConfigurableApplicationContext ctx;

    public AmqpBaselineDemo(ProtocolProperties props, ConfigurableApplicationContext ctx) {
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

            // ---- 1. 协议版本信息 ----
            log.info("====== 协议信息 ======");
            // amqp-client 默认且只讲 AMQP 0-9-1；AMQP 1.0 需另外的客户端（如 qpid），本 module 不引入
            log.info("本 demo 协议: AMQP 0-9-1（com.rabbitmq:amqp-client 默认且唯一支持的协议，RabbitMQ 母语）");
            log.info("AMQP 端口: {}（0-9-1 与 1.0 共用，靠连接初始的协议头协商版本）", ConnectionFactory.DEFAULT_AMQP_PORT);

            Map<String, Object> server = connection.getServerProperties();
            log.info("Broker: {} {}（cluster={}, platform={}）",
                    server.get("product"), server.get("version"),
                    server.get("cluster_name"), server.get("platform"));
            Map<String, Object> client = connection.getClientProperties();
            log.info("Client: {} {}", client.get("product"), client.get("version"));

            // ---- 2. AMQP 0-9-1 收发一条：临时 exclusive 队列，发一条、拉一条、ACK ----
            log.info("====== AMQP 0-9-1 收发基线 ======");
            String queue = channel.queueDeclare().getQueue(); // 临时、exclusive、auto-delete，每次全新
            log.info("声明临时队列: {}", queue);

            String message = "hello from amqp 0-9-1 @ " + LocalTime.now();
            channel.basicPublish("", queue,
                    MessageProperties.PERSISTENT_TEXT_PLAIN,
                    message.getBytes(StandardCharsets.UTF_8));
            log.info("[x] 已发送: {}", message);

            // 拉模式（basicGet）：demo 跑一个来回即可，无需注册异步消费者
            GetResponse response = channel.basicGet(queue, false); // autoAck=false，手动 ACK
            if (response != null) {
                String body = new String(response.getBody(), StandardCharsets.UTF_8);
                log.info("[o] 已收到: {}", body);
                channel.basicAck(response.getEnvelope().getDeliveryTag(), false);
            } else {
                log.warn("[?] 未取到消息（队列空）");
            }
        }

        // 一个来回跑完，关闭上下文、进程退出（spring.main.keep-alive 已设，demo 显式收尾）
        ctx.close();
    }
}

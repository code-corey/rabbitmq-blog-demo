package io.github.codecorey.progmodel.producer;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.MessageProperties;
import io.github.codecorey.progmodel.config.RabbitMqProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

/**
 * 对应博客 Step 1～5：建连 → 声明 Exchange / Queue / Binding → basicPublish 发送。
 *
 * <p>本类同时是整个演示的拓扑"所有者"：{@link #open()} 里一次性声明主交换机（带 alternate-exchange 兜底）、
 * 备选交换机、主队列（Quorum）、备选队列及全部绑定。其余 Consumer 假设拓扑已就绪，不再重复声明。
 *
 * <p>逐字复刻博客 Step 2/3/4/5 的方法签名：
 * <pre>
 * channel.exchangeDeclare(exchange, type, durable, autoDelete, arguments);
 * channel.queueDeclare(queue, durable, exclusive, autoDelete, arguments);
 * channel.queueBind(queue, exchange, routingKey);
 * channel.basicPublish(exchange, routingKey, props, message.getBytes("UTF-8"));
 * </pre>
 */
@Component
public class DemoProducer {

    private static final Logger log = LoggerFactory.getLogger(DemoProducer.class);

    private final ConnectionFactory factory;
    private final RabbitMqProperties props;

    private Connection connection;
    private Channel channel;

    public DemoProducer(ConnectionFactory factory, RabbitMqProperties props) {
        this.factory = factory;
        this.props = props;
    }

    /** Step 1：建连拿 Channel；Step 2～4：声明拓扑。 */
    public void open() throws IOException, TimeoutException {
        // Step 1：创建 Connection，获取 Channel（博客七步骨架第 1 步）
        connection = factory.newConnection();
        channel = connection.createChannel();
        declareTopology();
    }

    /** Step 2/3/4：声明 Exchange / Queue / Binding。 */
    private void declareTopology() throws IOException {
        String ex = props.getExchange();
        String ae = props.getAlternateExchange();
        String queue = props.getQueue();
        String aeQueue = props.getAlternateQueue();
        String routingKey = props.getRoutingKey();

        // Step 2：声明 Exchange —— 主交换机（DIRECT），带 alternate-exchange 兜底参数（对应博客第四节完整示例）
        Map<String, Object> exArgs = new HashMap<>();
        exArgs.put("alternate-exchange", ae);
        channel.exchangeDeclare(ex, BuiltinExchangeType.DIRECT, true, false, exArgs);
        // 备选交换机：博客原文用 DIRECT；这里改用 FANOUT 才能真正"兜底"接收所有不可路由消息（不论 routingKey）
        channel.exchangeDeclare(ae, BuiltinExchangeType.FANOUT, true, false, null);

        // Step 3：声明 Queue —— 主队列用 Quorum（x-queue-type=quorum, durable=true, exclusive=false），对应博客 Step 3
        Map<String, Object> queueArgs = new HashMap<>();
        if ("quorum".equalsIgnoreCase(props.getQueueType())) {
            queueArgs.put("x-queue-type", "quorum");
            // durable 必须为 true，exclusive 必须为 false（博客注释）
            log.info("[拓扑] 主队列 {} 声明为 Quorum（单节点仅作演示，无高可用意义）", queue);
        }
        channel.queueDeclare(queue, true, false, false, queueArgs);
        // 备选队列用 Classic durable，仅用于观察 alternate-exchange 兜底落到的消息
        channel.queueDeclare(aeQueue, true, false, false, null);

        // Step 4：声明 Binding
        channel.queueBind(queue, ex, routingKey);
        channel.queueBind(aeQueue, ae, ""); // FANOUT 交换机忽略 routingKey

        log.info("[拓扑] exchange={} (DIRECT, AE={}), queue={} ({}), ae-queue={}, 绑定 rk={}",
                ex, ae, queue, props.getQueueType(), aeQueue, routingKey);
    }

    /**
     * Step 5：basicPublish 发送消息。用 Builder 构建 BasicProperties（deliveryMode + priority，
     * 对应博客 Step 5），并附带 correlationId 便于追踪。
     */
    public void send(String routingKey, String correlationId, String body) throws IOException {
        // 逐字复刻博客 Step 5 的 Builder 用法
        AMQP.BasicProperties.Builder builder = new AMQP.BasicProperties.Builder();
        builder.deliveryMode(MessageProperties.PERSISTENT_TEXT_PLAIN.getDeliveryMode());
        builder.priority(MessageProperties.PERSISTENT_TEXT_PLAIN.getPriority());
        builder.correlationId(correlationId);
        AMQP.BasicProperties prop = builder.build();

        channel.basicPublish(props.getExchange(), routingKey, prop, body.getBytes(StandardCharsets.UTF_8));
        log.info("[x] 已发送: exchange={}, rk={}, body={}, correlationId={}",
                props.getExchange(), routingKey, body, correlationId);
    }

    public void close() throws IOException, TimeoutException {
        // Step 7：关闭连接（channel 先关，connection 后关）
        if (channel != null && channel.isOpen()) {
            channel.close();
        }
        if (connection != null && connection.isOpen()) {
            connection.close();
        }
    }
}

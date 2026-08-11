package io.github.codecorey.dlxdelay.runner;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.MessageProperties;
import io.github.codecorey.dlxdelay.config.RabbitMqProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.LocalTime;
import java.util.HashMap;
import java.util.Map;

/**
 * 对应博客《死信队列与延迟队列》第五节——TTL + DLX 实现延迟队列（订单关单链路）。
 *
 * <p>启动后一条龙完成：声明拓扑 → 注册 process.queue 的 Consumer → 向 delay.exchange 发送订单关单任务。
 * 消息在 delay.queue 中等待 TTL 到期成为死信，由 process.exchange 转发到 process.queue，
 * 最终被 Consumer 消费（打印死信 header）。
 *
 * <p>架构：
 * <pre>
 * Producer → delay.exchange → delay.queue（x-message-ttl + x-dead-letter-exchange，无 Consumer）
 *          → TTL 到期死信 → process.exchange → process.queue → Consumer 执行关单
 * </pre>
 * 纯队列 arguments 实现，<b>无需插件</b>。注意：死信转发<b>不经发送端 Confirm</b>
 * （见博客 3.3 节），无法保证与 Producer 侧同等安全级别。
 */
@Component
public class DelayQueueRunner implements CommandLineRunner {

    private static final Logger log = LoggerFactory.getLogger(DelayQueueRunner.class);

    private final RabbitMqProperties props;

    public DelayQueueRunner(RabbitMqProperties props) {
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

        // === 1. 先声明 process.exchange（死信交换机）——必须先存在，否则 delay.queue
        //         声明 x-dead-letter-exchange 时会因找不到交换机而失败（PRECONDITION_FAILED） ===
        channel.exchangeDeclare(props.getProcessExchange(), BuiltinExchangeType.DIRECT, true);
        // process.queue：普通队列，由 Consumer 消费执行关单（durable=true，博客第一节建议）
        channel.queueDeclare(props.getProcessQueue(), true, false, false, null);
        channel.queueBind(props.getProcessQueue(), props.getProcessExchange(), props.getProcessRoutingKey());

        // === 2. 声明 delay.exchange（Producer 投递目标） ===
        channel.exchangeDeclare(props.getDelayExchange(), BuiltinExchangeType.DIRECT, true);

        // === 3. delay.queue：核心参数 x-message-ttl + x-dead-letter-exchange（博客第五节）===
        Map<String, Object> queueArgs = new HashMap<>();
        // 消息 TTL（毫秒）：博客原文 1800000（30 分钟），演示用 5 秒
        queueArgs.put("x-message-ttl", props.getMessageTtl());
        // 死信交换机：TTL 到期后转发到 process.exchange
        queueArgs.put("x-dead-letter-exchange", props.getProcessExchange());
        // x-dead-letter-routing-key：设置后死信转移时用该 key（覆盖原 key）。
        // 不设则默认保留原 routing key（此处原 key 为 delay.order，与 process.queue 的绑定 key
        // process.order 不匹配会路由不到，故这里显式指定 —— 详见博客 3.3 节）
        queueArgs.put("x-dead-letter-routing-key", props.getProcessRoutingKey());
        channel.queueDeclare(props.getDelayQueue(), true, false, false, queueArgs);
        channel.queueBind(props.getDelayQueue(), props.getDelayExchange(), props.getDelayRoutingKey());

        log.info("=== 拓扑就绪：delay.queue={}（TTL={}ms → DLX={}）, process.queue={} ===",
                props.getDelayQueue(), props.getMessageTtl(),
                props.getProcessExchange(), props.getProcessQueue());

        // === 4. 注册 process.queue 的 Consumer（TTL 到期后死信到达此处执行关单）===
        channel.basicQos(1);
        channel.basicConsume(props.getProcessQueue(), false, new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope,
                                       AMQP.BasicProperties properties, byte[] body)
                    throws IOException {
                Map<String, Object> headers = properties.getHeaders();
                // 死信诊断 header（首次成为死信时写入的三个不可变属性，见博客第四节）
                String reason = headerStr(headers, "x-first-death-reason");   // expired / rejected / maxlen
                String fromQueue = headerStr(headers, "x-first-death-queue");  // 来源队列
                String fromExchange = headerStr(headers, "x-first-death-exchange"); // 来源交换机
                String content = new String(body, StandardCharsets.UTF_8);

                log.info("[关单] 收到死信: body={}, x-first-death-reason={}, x-first-death-queue={}, "
                                + "x-first-death-exchange={}, 接收时间={}",
                        content, reason, fromQueue, fromExchange, LocalTime.now());
                channel.basicAck(envelope.getDeliveryTag(), false);
            }
        });

        // === 5. 向 delay.exchange 发送 3 个订单关单任务（间隔 1 秒，便于观察错峰到期）===
        //        死信转发不经发送端 Confirm（博客 3.3 节），此处仅做普通持久化投递
        for (int i = 1; i <= 3; i++) {
            String message = "订单 ORDER-00" + i + " 关单任务";
            channel.basicPublish(props.getDelayExchange(), props.getDelayRoutingKey(),
                    MessageProperties.PERSISTENT_TEXT_PLAIN,
                    message.getBytes(StandardCharsets.UTF_8));
            log.info("[下单] 已发送到 delay.exchange: {}, 发送时间={}", message, LocalTime.now());
            if (i < 3) {
                Thread.sleep(1000L);
            }
        }

        log.info("=== 全部已投递，等待 {}ms 后死信到达 process.queue（keep-alive 保活） ===",
                props.getMessageTtl());
        // run() 返回后，进程由 spring.main.keep-alive 保活，Consumer 持续消费死信
    }

    /** 安全读取死信 header（LongString → toString），缺失时返回 "(无)"。 */
    private static String headerStr(Map<String, Object> headers, String key) {
        Object v = headers == null ? null : headers.get(key);
        return v == null ? "(无)" : v.toString();
    }
}

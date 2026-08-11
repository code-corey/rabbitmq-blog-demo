package io.github.codecorey.patterns;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.MessageProperties;
import io.github.codecorey.patterns.config.RabbitMqProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * 依次演示博客《RabbitMQ 常用消息场景》六种模式 + Publisher Confirms（原生 amqp-client）。
 *
 * <p>每个 demo 独立建连、独立清理（try-with-resources）；消费端用 {@link CountDownLatch}
 * 等待投递完成后再进入下一场景，保证演示顺序且可观测。连接参数由
 * {@link RabbitMqProperties} + application.yml 注入，默认 localhost:5672/admin/admin/vhost=/。
 */
@Component
public class PatternsDemoRunner implements CommandLineRunner {

    private static final Logger log = LoggerFactory.getLogger(PatternsDemoRunner.class);

    /** 场景1：默认交换机直连队列（博客「一、Hello World」）。 */
    private static final String HELLO_QUEUE = "pattern.hello";
    /** 场景2：工作队列（博客「二、Work Queues」），durable=true。 */
    private static final String TASK_QUEUE = "pattern.task_queue";
    /** 场景3：fanout 广播（博客「三、Publish/Subscribe」）。 */
    private static final String FANOUT_EXCHANGE = "pattern.logs";
    /** 场景4：direct 精确路由（博客「四、Routing」）。 */
    private static final String DIRECT_EXCHANGE = "pattern.direct_logs";
    /** 场景5：topic 通配符（博客「五、Topics」）。 */
    private static final String TOPIC_EXCHANGE = "pattern.topic_logs";
    /** 场景6：headers 头部路由（博客「七、Headers」）。 */
    private static final String HEADERS_EXCHANGE = "pattern.header_logs";

    private final ConnectionFactory connectionFactory;

    public PatternsDemoRunner(ConnectionFactory connectionFactory) {
        this.connectionFactory = connectionFactory;
    }

    @Override
    public void run(String... args) throws Exception {
        log.info("========== RabbitMQ 六种消息场景演示（原生 amqp-client） ==========");
        demoHello();
        demoWork();
        demoFanout();
        demoDirect();
        demoTopic();
        demoHeaders();
        demoConfirms();
        log.info("========== 全部场景演示完成（进程靠 spring.main.keep-alive 保活，Ctrl+C 退出） ==========");
    }

    // ====================================================================
    // 场景1：Hello World —— 默认交换机 "" + routingKey=队列名，autoAck=true
    // 最简单：Producer 发到指定 Queue，不经 Exchange。
    // ====================================================================
    private void demoHello() throws Exception {
        log.info("【场景1 Hello World】默认交换机 \"\" + routingKey=队列名，autoAck=true");
        try (Connection conn = connectionFactory.newConnection();
             Channel ch = conn.createChannel()) {

            // 非持久化队列：queueDeclare(name, durable, exclusive, autoDelete, args)
            ch.queueDeclare(HELLO_QUEUE, false, false, false, null);

            CountDownLatch latch = new CountDownLatch(1);
            // autoAck=true：Broker 投递即视为确认，consumer 内无需 basicAck
            ch.basicConsume(HELLO_QUEUE, true, new DefaultConsumer(ch) {
                @Override
                public void handleDelivery(String consumerTag, Envelope envelope,
                                           AMQP.BasicProperties properties, byte[] body) {
                    log.info("[Hello 收到] {}", new String(body, StandardCharsets.UTF_8));
                    latch.countDown();
                }
            });

            // 默认交换机 ""：routingKey 即队列名，消息直连该队列
            String message = "Hello World!";
            ch.basicPublish("", HELLO_QUEUE, null, message.getBytes(StandardCharsets.UTF_8));
            log.info("[Hello 发送] {}", message);

            latch.await(3, TimeUnit.SECONDS);
        }
    }

    // ====================================================================
    // 场景2：Work Queue —— durable + PERSISTENT_TEXT_PLAIN + basicQos(1) + 手动 ack
    // 一个任务队列，多个 Worker 竞争消费（fair dispatch / round-robin）。
    // 三个易错点：① durable 队列 ② PERSISTENT_TEXT_PLAIN ③ basicQos(1)+手动 ack
    // ====================================================================
    private void demoWork() throws Exception {
        log.info("【场景2 Work Queue】durable=true + PERSISTENT_TEXT_PLAIN + basicQos(1) + 手动 ack");
        try (Connection conn = connectionFactory.newConnection();
             Channel ch = conn.createChannel()) {

            // 易错点①：durable=true，队列持久化（重启不丢队列定义）
            ch.queueDeclare(TASK_QUEUE, true, false, false, null);

            int workerCount = 2;
            int taskCount = 6;
            CountDownLatch latch = new CountDownLatch(taskCount);

            for (int i = 1; i <= workerCount; i++) {
                final String worker = "Worker-" + i;
                Channel workerChannel = conn.createChannel();
                // 易错点③：basicQos(1) 限制未 ack 消息数，Broker 超过 prefetch 不再投递
                workerChannel.basicQos(1);
                // 易错点②：autoAck=false → 必须在 handleDelivery 里手动 basicAck，否则形成毒消息
                workerChannel.basicConsume(TASK_QUEUE, false, new DefaultConsumer(workerChannel) {
                    @Override
                    public void handleDelivery(String consumerTag, Envelope envelope,
                                               AMQP.BasicProperties properties, byte[] body)
                            throws IOException {
                        log.info("[{} 处理] {}", worker, new String(body, StandardCharsets.UTF_8));
                        workerChannel.basicAck(envelope.getDeliveryTag(), false);
                        latch.countDown();
                    }
                });
            }

            // 发布持久化任务：MessageProperties.PERSISTENT_TEXT_PLAIN（delivery_mode=2）
            for (int i = 1; i <= taskCount; i++) {
                String task = "task-" + i;
                ch.basicPublish("", TASK_QUEUE,
                        MessageProperties.PERSISTENT_TEXT_PLAIN,
                        task.getBytes(StandardCharsets.UTF_8));
            }
            log.info("[Work 发送] {} 条持久化任务，{} 个 Worker 竞争消费（round-robin）", taskCount, workerCount);

            latch.await(10, TimeUnit.SECONDS);
        }
    }

    // ====================================================================
    // 场景3：fanout 广播 —— 忽略 routingKey，转发到所有绑定队列
    // 每个 Consumer 各自声明临时 Queue 并绑定到 fanout Exchange。
    // ====================================================================
    private void demoFanout() throws Exception {
        log.info("【场景3 fanout】广播：一条消息复制到所有绑定的临时队列");
        try (Connection conn = connectionFactory.newConnection();
             Channel ch = conn.createChannel()) {

            ch.exchangeDeclare(FANOUT_EXCHANGE, "fanout");

            int subscriberCount = 3;
            CountDownLatch latch = new CountDownLatch(subscriberCount);
            for (int i = 1; i <= subscriberCount; i++) {
                final String sub = "Sub-" + i;
                // 服务端生成临时队列名（queueDeclare() 无参：非持久、独占、自动删除）
                String queueName = ch.queueDeclare().getQueue();
                ch.queueBind(queueName, FANOUT_EXCHANGE, "");
                ch.basicConsume(queueName, true, loggingConsumer(ch, sub, latch));
            }

            String message = "fanout broadcast";
            // fanout 忽略 routingKey，转发到所有绑定队列
            ch.basicPublish(FANOUT_EXCHANGE, "", null, message.getBytes(StandardCharsets.UTF_8));
            log.info("[fanout 发送] {} → {} 个订阅者各收一份", message, subscriberCount);

            latch.await(3, TimeUnit.SECONDS);
        }
    }

    // ====================================================================
    // 场景4：direct 精确路由 —— routingKey 完全匹配 Binding Key
    // 如 error 进告警队列，info 进归档队列；一个队列可绑定多个 routingKey。
    // ====================================================================
    private void demoDirect() throws Exception {
        log.info("【场景4 direct】精确路由：routingKey 完全匹配 Binding Key");
        try (Connection conn = connectionFactory.newConnection();
             Channel ch = conn.createChannel()) {

            ch.exchangeDeclare(DIRECT_EXCHANGE, "direct");

            // Alert 队列只关心 error
            String alertQueue = ch.queueDeclare().getQueue();
            ch.queueBind(alertQueue, DIRECT_EXCHANGE, "error");

            // Archive 队列同时绑定 info 和 error（一个队列绑定多个 routingKey）
            String archiveQueue = ch.queueDeclare().getQueue();
            ch.queueBind(archiveQueue, DIRECT_EXCHANGE, "info");
            ch.queueBind(archiveQueue, DIRECT_EXCHANGE, "error");

            // 期望命中：error→Alert(1) + info→Archive(1) + error→Archive(1) = 3 条
            CountDownLatch latch = new CountDownLatch(3);
            ch.basicConsume(alertQueue, true, loggingConsumer(ch, "Alert", latch));
            ch.basicConsume(archiveQueue, true, loggingConsumer(ch, "Archive", latch));

            ch.basicPublish(DIRECT_EXCHANGE, "error", null,
                    "[error] something broke".getBytes(StandardCharsets.UTF_8));
            ch.basicPublish(DIRECT_EXCHANGE, "info", null,
                    "[info] all good".getBytes(StandardCharsets.UTF_8));
            log.info("[direct 发送] routingKey=error + routingKey=info");

            latch.await(3, TimeUnit.SECONDS);
        }
    }

    // ====================================================================
    // 场景5：topic 通配符 —— '*' 匹配恰好一个词，'#' 匹配零或多个词
    // routingKey 为点分单词，Binding 支持通配符，如 order.* / *.error / order.#
    // ====================================================================
    private void demoTopic() throws Exception {
        log.info("【场景5 topic】通配符：'*' 匹配恰好一个词，'#' 匹配零或多个词");
        try (Connection conn = connectionFactory.newConnection();
             Channel ch = conn.createChannel()) {

            ch.exchangeDeclare(TOPIC_EXCHANGE, "topic");

            String orderQueue = ch.queueDeclare().getQueue();
            ch.queueBind(orderQueue, TOPIC_EXCHANGE, "order.*"); // order 下恰好两层

            String errorQueue = ch.queueDeclare().getQueue();
            ch.queueBind(errorQueue, TOPIC_EXCHANGE, "*.error"); // 所有 error

            String allOrderQueue = ch.queueDeclare().getQueue();
            ch.queueBind(allOrderQueue, TOPIC_EXCHANGE, "order.#"); // order 下全部层级

            // 4 条消息将共命中 5 次：
            //   order.payment.success → order.#（* 不匹配 3 段）           = 1
            //   order.shipped         → order.* + order.#                 = 2
            //   payment.error         → *.error                           = 1
            //   order.refund.error    → order.#（order.* 与 *.error 不匹配 3 段）= 1
            CountDownLatch latch = new CountDownLatch(5);
            ch.basicConsume(orderQueue, true, loggingConsumer(ch, "order.*", latch));
            ch.basicConsume(errorQueue, true, loggingConsumer(ch, "*.error", latch));
            ch.basicConsume(allOrderQueue, true, loggingConsumer(ch, "order.#", latch));

            String[] routingKeys = {
                    "order.payment.success",
                    "order.shipped",
                    "payment.error",
                    "order.refund.error"
            };
            for (String routingKey : routingKeys) {
                ch.basicPublish(TOPIC_EXCHANGE, routingKey, null,
                        ("topic-msg:" + routingKey).getBytes(StandardCharsets.UTF_8));
            }
            log.info("[topic 发送] {}", String.join(" / ", routingKeys));

            latch.await(3, TimeUnit.SECONDS);
        }
    }

    // ====================================================================
    // 场景6：headers 头部路由 —— 忽略 routingKey，按消息 Header 键值对匹配
    // x-match=any（任一命中）/ all（全部命中），复刻博客第七节绑定与发送代码。
    // ====================================================================
    private void demoHeaders() throws Exception {
        log.info("【场景6 headers】按消息 Header 匹配：x-match=any/all，忽略 routingKey");
        try (Connection conn = connectionFactory.newConnection();
             Channel ch = conn.createChannel()) {

            ch.exchangeDeclare(HEADERS_EXCHANGE, BuiltinExchangeType.HEADERS);

            // 绑定①：x-match=any（任一 header 命中即转发）—— 复刻博客绑定代码
            String anyQueue = ch.queueDeclare().getQueue();
            Map<String, Object> anyHeaders = new HashMap<>();
            anyHeaders.put("x-match", "any");
            anyHeaders.put("loglevel", "info");
            anyHeaders.put("buslevel", "product");
            anyHeaders.put("syslevel", "admin");
            ch.queueBind(anyQueue, HEADERS_EXCHANGE, "", anyHeaders);

            // 绑定②：x-match=all（所有 header 必须全部命中）
            String allQueue = ch.queueDeclare().getQueue();
            Map<String, Object> allHeaders = new HashMap<>();
            allHeaders.put("x-match", "all");
            allHeaders.put("loglevel", "error");
            allHeaders.put("buslevel", "product");
            allHeaders.put("syslevel", "admin");
            ch.queueBind(allQueue, HEADERS_EXCHANGE, "", allHeaders);

            // 发送：headers = {loglevel=error, buslevel=product, syslevel=admin}
            //   → any 命中（buslevel/syslevel 命中）+ all 命中（三项全中）= 2 条
            CountDownLatch latch = new CountDownLatch(2);
            ch.basicConsume(anyQueue, true, loggingConsumer(ch, "Headers[any]", latch));
            ch.basicConsume(allQueue, true, loggingConsumer(ch, "Headers[all]", latch));

            Map<String, Object> messageHeaders = new HashMap<>();
            messageHeaders.put("loglevel", "error");
            messageHeaders.put("buslevel", "product");
            messageHeaders.put("syslevel", "admin");

            // 复刻博客发送：deliveryMode=PERSISTENT_TEXT_PLAIN + headers
            AMQP.BasicProperties props = new AMQP.BasicProperties.Builder()
                    .deliveryMode(MessageProperties.PERSISTENT_TEXT_PLAIN.getDeliveryMode())
                    .headers(messageHeaders)
                    .build();

            String body = "header-msg";
            ch.basicPublish(HEADERS_EXCHANGE, "", props, body.getBytes(StandardCharsets.UTF_8));
            log.info("[headers 发送] {} headers={}", body, messageHeaders);

            latch.await(3, TimeUnit.SECONDS);
        }
    }

    // ====================================================================
    // 场景7：Publisher Confirms —— 发送端可靠性确认
    // basicPublish 无返回值，Producer 不知道是否到 Broker；confirmSelect 开启确认。
    // 演示：① 单条同步 waitForConfirmsOrDie(5_000) ② 异步 addConfirmListener(seq, multiple)
    // ====================================================================
    private void demoConfirms() throws Exception {
        log.info("【场景7 Publisher Confirms】confirmSelect + waitForConfirmsOrDie / 异步 ConfirmListener");

        // ---- (a) 单条同步确认：每发一条 waitForConfirmsOrDie(5_000) ----
        try (Connection conn = connectionFactory.newConnection();
             Channel ch = conn.createChannel()) {
            ch.confirmSelect(); // 开启 Publisher Confirms
            String queue = ch.queueDeclare().getQueue();

            int n = 3;
            for (int i = 1; i <= n; i++) {
                String body = "confirm-sync-" + i;
                ch.basicPublish("", queue, null, body.getBytes(StandardCharsets.UTF_8));
                // 阻塞 Channel 直至 Broker 确认或超时抛异常；吞吐最低、最安全感知单条
                ch.waitForConfirmsOrDie(5_000);
                log.info("[Confirms 同步] 第 {} 条已确认", i);
            }
        }

        // ---- (b) 异步确认（推荐）：addConfirmListener(sequenceNumber, multiple) ----
        try (Connection conn = connectionFactory.newConnection();
             Channel ch = conn.createChannel()) {
            ch.confirmSelect();
            String queue = ch.queueDeclare().getQueue();

            // 应用自行维护「发布序号 → 消息体」映射，便于在 nack 时定位
            ConcurrentSkipListMap<Long, String> outstanding = new ConcurrentSkipListMap<>();
            ch.addConfirmListener(
                    (sequenceNumber, multiple) -> {
                        if (multiple) {
                            // multiple=true：确认 seq 及之前所有消息
                            outstanding.headMap(sequenceNumber + 1).clear();
                            log.info("[Confirms 异步] ack multiple，seq<={} 全部确认", sequenceNumber);
                        } else {
                            String body = outstanding.remove(sequenceNumber);
                            log.info("[Confirms 异步] ack seq={} body={}", sequenceNumber, body);
                        }
                    },
                    (sequenceNumber, multiple) ->
                            log.warn("[Confirms 异步] nack seq={} multiple={}", sequenceNumber, multiple)
            );

            int n = 5;
            for (int i = 1; i <= n; i++) {
                String body = "confirm-async-" + i;
                long seq = ch.getNextPublishSeqNo(); // 全局递增发布序号
                outstanding.put(seq, body);
                ch.basicPublish("", queue, null, body.getBytes(StandardCharsets.UTF_8));
            }
            log.info("[Confirms 异步] 已发送 {} 条，等待异步 ack...", n);

            // 给异步回调一点处理时间
            Thread.sleep(1_000);
            log.info("[Confirms 异步] 未确认剩余 {} 条", outstanding.size());
        }
    }

    /**
     * autoAck 演示用消费者：打印 routingKey 与消息体，命中后 {@code countdown}。
     *
     * <p>用于 fanout / direct / topic / headers 场景（均为 autoAck=true，无需手动 ack）。
     */
    private DefaultConsumer loggingConsumer(Channel ch, String tag, CountDownLatch latch) {
        return new DefaultConsumer(ch) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope,
                                       AMQP.BasicProperties properties, byte[] body) {
                log.info("[{} 收到] routingKey={} | {}", tag, envelope.getRoutingKey(),
                        new String(body, StandardCharsets.UTF_8));
                if (latch != null) {
                    latch.countDown();
                }
            }
        };
    }
}

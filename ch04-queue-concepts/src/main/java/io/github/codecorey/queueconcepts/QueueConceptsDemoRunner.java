package io.github.codecorey.queueconcepts;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.GetResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * 依次演示博客《RabbitMQ 队列核心概念》四个主题（原生 amqp-client）。
 *
 * <p>每个 demo 独立建连、独立清理（try-with-resources）；连接参数由
 * {@link io.github.codecorey.queueconcepts.config.RabbitMqProperties} + application.yml 注入，
 * 默认 localhost:5672/admin/admin/vhost=/。
 *
 * <p>演示内容（对应博客章节）：
 * <ol>
 *   <li>优先级队列 —— {@code x-max-priority=10}，高优先级消息插队投递（博客第五节）</li>
 *   <li>服务端命名队列 —— {@code queueDeclare("")} + Channel 记忆特性（博客第一节）</li>
 *   <li>临时/独占队列 —— exclusive + server-named，连接关即删（博客第六节）</li>
 *   <li>声明等价 —— PRECONDITION_FAILED 注释说明（博客第二节，不真触发）</li>
 * </ol>
 */
@Component
public class QueueConceptsDemoRunner implements CommandLineRunner {

    private static final Logger log = LoggerFactory.getLogger(QueueConceptsDemoRunner.class);

    /** 场景1：优先级队列（博客第五节），x-max-priority=10。 */
    private static final String PRIORITY_QUEUE = "queue.priority.demo";

    private final ConnectionFactory connectionFactory;

    public QueueConceptsDemoRunner(ConnectionFactory connectionFactory) {
        this.connectionFactory = connectionFactory;
    }

    @Override
    public void run(String... args) throws Exception {
        log.info("========== RabbitMQ 队列核心概念演示（原生 amqp-client） ==========");
        demoPriorityQueue();
        demoServerNamedQueue();
        demoExclusiveQueue();
        explainPreconditionFailed();
        log.info("========== 全部演示完成（进程靠 spring.main.keep-alive 保活，Ctrl+C 退出） ==========");
    }

    // ====================================================================
    // 场景1：优先级队列 —— x-max-priority=10，高优先级消息"插队"投递
    // 声明队列时设 x-max-priority（只能声明时设，policy 改不了）；发布时在消息
    // 属性里带 priority 字段。高优先级先投——这也是 FIFO 保序会被打乱的原因之一。
    // ====================================================================
    private void demoPriorityQueue() throws Exception {
        log.info("【场景1 优先级队列】声明 x-max-priority=10，高优先级消息插队投递");
        try (Connection conn = connectionFactory.newConnection();
             Channel ch = conn.createChannel()) {

            // x-max-priority 只能在 queueDeclare 时设定（policy 改不了，见博客第三节）
            Map<String, Object> args = Map.of("x-max-priority", 10);
            ch.queueDeclare(PRIORITY_QUEUE, true, false, false, args);
            ch.queuePurge(PRIORITY_QUEUE); // 清空上次运行残留，确保观察到的是本次投递顺序

            // 先不挂消费者，按"普通→紧急"顺序发布，让消息在队列里排好
            // 发布顺序故意是 1,1,9,5,1 —— 观察消费时是否按 9,5,1,1,1 投递
            publishWithPriority(ch, 1, "普通-A");
            publishWithPriority(ch, 1, "普通-B");
            publishWithPriority(ch, 9, "【紧急】");
            publishWithPriority(ch, 5, "较急");
            publishWithPriority(ch, 1, "普通-C");
            log.info("[优先级] 已发布 5 条（发布顺序 priority=1,1,9,5,1），现在挂消费者观察投递顺序");

            CountDownLatch latch = new CountDownLatch(5);
            ch.basicQos(1); // 每次只投一条，确保观察到的顺序即 Broker 的出队顺序
            ch.basicConsume(PRIORITY_QUEUE, false, new DefaultConsumer(ch) {
                @Override
                public void handleDelivery(String consumerTag, Envelope envelope,
                                           AMQP.BasicProperties properties, byte[] body)
                        throws IOException {
                    log.info("[优先级 收到] priority={} | {}", properties.getPriority(),
                            new String(body, StandardCharsets.UTF_8));
                    ch.basicAck(envelope.getDeliveryTag(), false);
                    latch.countDown();
                }
            });

            latch.await(5, TimeUnit.SECONDS);
            log.info("[优先级] 若上面投递顺序为 9→5→1→1→1，说明高优先级成功插队（同优先级仍 FIFO）");
        }
    }

    private void publishWithPriority(Channel ch, int priority, String body) throws IOException {
        AMQP.BasicProperties props = new AMQP.BasicProperties.Builder()
                .priority(priority)
                .build();
        ch.basicPublish("", PRIORITY_QUEUE, props, body.getBytes(StandardCharsets.UTF_8));
    }

    // ====================================================================
    // 场景2：服务端命名队列 —— queueDeclare("") 拿 Broker 生成的名字
    // 传空串 "" 由 Broker 生成唯一名（形如 amq.gen-xxxx）；同一 Channel 会"记住"
    // 上一次服务端生成的队列名，后续用空串当队列名指的就是它，无需把名字传来传去。
    // ====================================================================
    private void demoServerNamedQueue() throws Exception {
        log.info("【场景2 服务端命名队列】queueDeclare(\"\") + Channel 记忆特性");
        try (Connection conn = connectionFactory.newConnection();
             Channel ch = conn.createChannel()) {

            // 传空串 "" → Broker 生成唯一名（形如 amq.gen-Jz_Q...）
            String generatedName = ch.queueDeclare("", false, true, true, null).getQueue();
            log.info("[服务端命名] Broker 生成队列名: {}", generatedName);

            // 用真实名发布一条消息（默认交换机 routingKey = 队列名）
            ch.basicPublish("", generatedName, null, "server-named ping".getBytes(StandardCharsets.UTF_8));

            // Channel 记忆：用空串 "" 消费 → Broker 解析为上面生成的 generatedName
            CountDownLatch latch = new CountDownLatch(1);
            ch.basicConsume("", true, new DefaultConsumer(ch) {
                @Override
                public void handleDelivery(String consumerTag, Envelope envelope,
                                           AMQP.BasicProperties properties, byte[] body)
                        throws IOException {
                    log.info("[服务端命名] basicConsume(\"\") 收到: {} （空串被解析为 {}）",
                            new String(body, StandardCharsets.UTF_8), generatedName);
                    latch.countDown();
                }
            });
            log.info("[服务端命名] 已用 basicConsume(\"\") 注册消费者，空串指向 {}", generatedName);

            latch.await(3, TimeUnit.SECONDS);
            log.info("[服务端命名] 结论：临时队列用完在同一 Channel 操作即可，无需把名字传来传去");
        }
    }

    // ====================================================================
    // 场景3：临时/独占队列 —— exclusive + server-named，连接关即删
    // channel.queueDeclare() 无参 == queueDeclare("", false, true, true, null)
    // exclusive 队列连接私有，只有声明它的连接能访问；连接一关 Broker 立即删除。
    // （博客第六节：exclusive 一定是 Classic、连接私有、durable 无意义）
    // ====================================================================
    private void demoExclusiveQueue() throws Exception {
        log.info("【场景3 临时/独占队列】exclusive + server-named，连接关即删");
        try (Connection conn = connectionFactory.newConnection();
             Channel ch = conn.createChannel()) {

            // 临时队列三合一：服务端命名 + exclusive + autoDelete
            // channel.queueDeclare() 无参版 == queueDeclare("", false, true, true, null)
            String tempQueue = ch.queueDeclare().getQueue();
            log.info("[临时队列] 无参 queueDeclare() → 服务端生成名: {}", tempQueue);
            log.info("[临时队列] 等价显式写法: queueDeclare(\"\", false, true, true, null)");

            // 同一连接内可正常收发（exclusive 只拒绝其他连接）
            ch.basicPublish("", tempQueue, null, "exclusive ping".getBytes(StandardCharsets.UTF_8));
            GetResponse resp = ch.basicGet(tempQueue, true);
            log.info("[临时队列] basicGet 取回: {}",
                    resp == null ? "(空)" : new String(resp.getBody(), StandardCharsets.UTF_8));

            log.info("[临时队列] 结论：try-with-resources → conn.close() 后此队列自动删除（exclusive 绑死连接）");
        }
    }

    // ====================================================================
    // 场景4：PRECONDITION_FAILED（声明等价） —— 仅注释说明，不真触发异常
    // 用相同名字重复声明队列时，所有属性(durable/exclusive/autoDelete/arguments)必须一致；
    // 不一致 → 通道级异常，reply code 406 PRECONDITION_FAILED，Channel 随即关闭。
    // ====================================================================
    private void explainPreconditionFailed() {
        log.info("【场景4 PRECONDITION_FAILED】声明等价铁律（仅说明，不触发异常）");
        log.info("[声明等价] 同名重复声明队列时，durable/exclusive/autoDelete/arguments 必须全部一致；");
        log.info("[声明等价] 不一致 → 通道级异常 reply code 406 PRECONDITION_FAILED，Channel 随即关闭。");
        log.info("[声明等价] 典型踩坑：先用 durable=false 建过队列，代码改成 durable=true 再声明 → 406。");
        log.info("[声明等价] 解法：先在管理台删掉旧队列，再用新参数声明（唯一解法）。");
        log.info("[声明等价] 例外：queue type 等价检查可放宽，或用 Virtual Host 默认队列类型（DQT）规避。");

        // 下面这段代码会触发 PRECONDITION_FAILED（已注释，不执行，以免中断后续演示）：
        // try (Connection conn = connectionFactory.newConnection();
        //      Channel ch = conn.createChannel()) {
        //     ch.queueDeclare("precondition.demo", false, false, false, null); // durable=false
        //     // 同名再声明但 durable=true → 属性不一致 → 406 PRECONDITION_FAILED → Channel 关闭
        //     ch.queueDeclare("precondition.demo", true, false, false, null);  // durable=true
        // }
    }
}

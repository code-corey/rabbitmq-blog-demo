package io.github.codecorey.rpc;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * RPC 客户端 —— 用 <b>direct reply-to</b> 收应答（博客第四节）。
 *
 * <p>direct reply-to 是 RabbitMQ 的"伪队列"机制：client 不再声明临时回复队列，
 * 而是直接消费固定名字 {@link #DIRECT_REPLY_TO}（{@code amq.rabbitmq.reply-to}）。
 * Broker 内部根本<b>没有</b>这个队列实体 —— 它把应答从 server 的通道进程直接递到 client 的
 * 通道进程，省掉元数据存储、消息缓冲和独立 Erlang 进程。
 *
 * <p>流程：
 * <ol>
 *   <li>构造时在 <b>no-ack 模式</b>下 {@code basicConsume(DIRECT_REPLY_TO, true, ...)} 收应答；</li>
 *   <li>每次 {@link #call(String)} 生成唯一 {@code correlationId}，注册到 {@link #pending}，
 *       发布请求时 {@code replyTo} 设成 {@link #DIRECT_REPLY_TO}；</li>
 *   <li>Broker 透明改写 {@code replyTo} 成 {@code amq.rabbitmq.reply-to.<不透明后缀>}（每连接唯一），
 *       server 把应答发到改写后的名字，Broker 直接送回本 client；</li>
 *   <li>回复消费者按 {@code correlationId} 把应答派发到对应等待队列，调用方解除阻塞。</li>
 * </ol>
 *
 * <p><b>direct reply-to 的硬规矩（博客 4.4）：</b>
 * <ul>
 *   <li>必须 no-ack 消费 {@code amq.rabbitmq.reply-to}（没有队列给你退回消息）；</li>
 *   <li>发布请求和消费应答必须用<b>同一个连接、同一个通道</b>（本类即共用 {@link #channel}）；</li>
 *   <li>每条通道最多一个 direct reply-to 消费者；</li>
 *   <li>应答是 <b>at-most-once（至多一次）</b>：client 断开即丢，重连后需自行重发；</li>
 *   <li>{@code amq.rabbitmq.reply-to} 不是真队列 —— 不可删、不在管理界面出现、
 *       {@code rabbitmqctl list_queues} 也看不到。</li>
 * </ul>
 */
public class RpcClient implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(RpcClient.class);

    /** direct reply-to 的伪队列名，固定值（Broker 自动改写为带后缀的内部地址）。 */
    public static final String DIRECT_REPLY_TO = "amq.rabbitmq.reply-to";

    /** 默认应答超时（博客 6.1：永远别让 client 无限等下去）。 */
    private static final long DEFAULT_TIMEOUT_MS = 10_000L;

    private final Connection connection;
    private final Channel channel;

    /** correlationId → 等待句柄：并发请求共用此回复通道，靠 correlationId 把应答派发到对应调用方。 */
    private final ConcurrentHashMap<String, BlockingQueue<String>> pending = new ConcurrentHashMap<>();

    public RpcClient(ConnectionFactory connectionFactory) throws Exception {
        // 注意：direct reply-to 要求"发布请求"和"消费应答"共用同一个连接+通道
        connection = connectionFactory.newConnection();
        channel = connection.createChannel();

        // 唯一一个回复消费者：no-ack 模式消费伪队列（无需 queueDeclare），按 correlationId 分发
        channel.basicConsume(DIRECT_REPLY_TO, true, new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope,
                                       AMQP.BasicProperties properties, byte[] body) {
                String corrId = properties.getCorrelationId();
                BlockingQueue<String> q = pending.get(corrId);
                if (q != null) {
                    q.offer(new String(body, StandardCharsets.UTF_8));
                }
            }
        });
    }

    /**
     * 发送一条 RPC 请求并阻塞等应答（默认超时 {@value #DEFAULT_TIMEOUT_MS} ms）。
     *
     * @param message 请求体
     * @return 应答体；若以 {@code ERROR:} 开头，说明 server 端业务异常（博客 6.3）
     * @throws RuntimeException 超时未收到应答时抛出（博客 6.1）
     */
    public String call(String message) throws Exception {
        return call(message, DEFAULT_TIMEOUT_MS);
    }

    /** 带超时版本的 RPC 调用。 */
    public String call(String message, long timeoutMs) throws Exception {
        final String corrId = UUID.randomUUID().toString();
        // 每次请求注册一个"信箱"，等待对应应答
        BlockingQueue<String> responseQ = new LinkedBlockingQueue<>(1);
        pending.put(corrId, responseQ);

        AMQP.BasicProperties props = new AMQP.BasicProperties.Builder()
                .correlationId(corrId)
                .replyTo(DIRECT_REPLY_TO)   // 关键：告诉 server 往哪回（Broker 会改写后缀）
                .build();

        channel.basicPublish("", RpcServer.RPC_QUEUE, props, message.getBytes(StandardCharsets.UTF_8));
        log.info("[Client 发送] {} (corrId={})", message, corrId);

        // 阻塞等待应答
        String result = responseQ.poll(timeoutMs, TimeUnit.MILLISECONDS);
        // 及时移除映射项，防内存泄漏（博客 6.2）
        pending.remove(corrId);
        if (result == null) {
            throw new RuntimeException("RPC 超时，未收到应答: " + message);
        }
        return result;
    }

    @Override
    public void close() throws IOException {
        connection.close();
    }
}

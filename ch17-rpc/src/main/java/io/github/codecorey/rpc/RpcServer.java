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
import java.util.concurrent.TimeoutException;

/**
 * RPC 服务端（博客第三节）：消费 {@link #RPC_QUEUE}，处理请求后把应答发回 {@code replyTo}。
 *
 * <p>支持两种请求（前缀协议，演示用）：
 * <ul>
 *   <li>{@code fib:N} —— 返回 fib(N) 的值（笨办法递归，模拟耗时计算，复刻官方教程）</li>
 *   <li>{@code upper:xxx} —— 返回 xxx 的大写形式</li>
 * </ul>
 *
 * <p>服务端只管把应答发到请求 {@code replyTo} 指定的地址（走默认交换机 {@code ""}，
 * 路由键即该地址），并原样回填 {@code correlationId}。它<b>不关心</b>那个地址是真队列
 * 还是 direct reply-to 的伪名字 —— 两种 client 都能照常工作（博客 4.3）。
 *
 * <p>可靠性要点（博客第六节）：
 * <ul>
 *   <li>{@code basicQos(1)} —— 公平分发，处理完再投下一条；</li>
 *   <li>异常也回应答（{@code ERROR:} 前缀），不让 client 干等到超时（博客 6.3）；</li>
 *   <li>先 {@code basicPublish} 发应答、后 {@code basicAck} 请求，保证"应答已发出"先于
 *       "请求已处理"（博客 6.4）。</li>
 * </ul>
 */
public class RpcServer implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(RpcServer.class);

    /** RPC 请求队列名（博客第三节 {@code rpc_queue}）。 */
    public static final String RPC_QUEUE = "rpc.queue";

    private final ConnectionFactory connectionFactory;
    private Connection connection;
    private Channel channel;

    public RpcServer(ConnectionFactory connectionFactory) {
        this.connectionFactory = connectionFactory;
    }

    /**
     * 建连并注册 {@link #RPC_QUEUE} 的消费者（autoAck=false，处理完手动 ack）。
     * 幂等：已启动则直接返回。
     */
    public void start() throws IOException, TimeoutException {
        if (channel != null) {
            return;
        }
        connection = connectionFactory.newConnection();
        channel = connection.createChannel();
        // 请求队列：durable=false 仅 demo 用，生产看 07 队列类型按需选
        channel.queueDeclare(RPC_QUEUE, false, false, false, null);
        // 公平分发：一次只分一条，见 05 篇 Work Queue
        channel.basicQos(1);

        channel.basicConsume(RPC_QUEUE, false, new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope,
                                       AMQP.BasicProperties properties, byte[] body) throws IOException {
                // 应答属性：correlationId 原样回填，replyTo 决定发到哪
                AMQP.BasicProperties replyProps = new AMQP.BasicProperties.Builder()
                        .correlationId(properties.getCorrelationId())
                        .build();
                String replyTo = properties.getReplyTo();
                String response = "";
                try {
                    String message = new String(body, StandardCharsets.UTF_8);
                    response = handle(message);
                    log.info("[Server 处理] {} -> {} (replyTo={}, corrId={})",
                            message, response, replyTo, properties.getCorrelationId());
                } catch (RuntimeException e) {
                    // 业务异常：把错误信息作为应答体回传，而不是吞掉（博客 6.3）
                    response = "ERROR: " + e.getMessage();
                    log.warn("[Server 异常] {}", e.toString());
                } finally {
                    // 先发应答、后 ack 请求（博客 6.4）：保证"应答已到 Broker"先于"请求已处理"
                    channel.basicPublish("", replyTo, replyProps,
                            response.getBytes(StandardCharsets.UTF_8));
                    channel.basicAck(envelope.getDeliveryTag(), false);
                }
            }
        });
        log.info("[Server 启动] 等待 RPC 请求，队列={}", RPC_QUEUE);
    }

    /** 前缀协议分发：{@code fib:N} / {@code upper:xxx}。 */
    private String handle(String message) {
        if (message.startsWith("fib:")) {
            int n = Integer.parseInt(message.substring(4).trim());
            return String.valueOf(fib(n));
        }
        if (message.startsWith("upper:")) {
            return message.substring(6).toUpperCase();
        }
        throw new IllegalArgumentException("无法识别的请求格式，期望 fib:N 或 upper:xxx");
    }

    /** 笨办法算斐波那契，只为模拟"耗时计算"（复刻官方教程）。 */
    private static int fib(int n) {
        if (n == 0) return 0;
        if (n == 1) return 1;
        return fib(n - 1) + fib(n - 2);
    }

    @Override
    public void close() throws IOException {
        if (connection != null) {
            connection.close();
        }
    }
}

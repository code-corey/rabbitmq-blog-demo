package io.github.codecorey.backlog.producer;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.MessageProperties;
import io.github.codecorey.backlog.config.BacklogProperties;
import io.github.codecorey.backlog.support.Connections;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 高速生产者：以最快速度向经典队列灌 {@code messageCount} 条持久化消息
 * （{@link MessageProperties#PERSISTENT_TEXT_PLAIN}，delivery_mode=2），制造大规模积压，
 * 触发「内存窗口越线 → 落盘 + 随机 I/O + 流控」的断崖。
 *
 * <p>注册 {@code BlockedListener}：当 Broker 内存到 high watermark、掐停生产者（publisher throttle）
 * 时打印告警，配合监控线程的发布速率曲线，即可直观看到「断崖」。
 */
@Component
public class HighSpeedProducer {

    private static final Logger log = LoggerFactory.getLogger(HighSpeedProducer.class);

    private final BacklogProperties props;
    private final AtomicLong publishedCount = new AtomicLong();
    private final AtomicBoolean blocked = new AtomicBoolean(false);

    public HighSpeedProducer(BacklogProperties props) {
        this.props = props;
    }

    /**
     * 全速发布全部消息，返回总耗时（毫秒）。发布过程中 {@code publishedCount} 持续增长，供监控线程采样。
     */
    public long sendAll() throws Exception {
        try (Connection connection = Connections.factory(props).newConnection("backlog-producer");
             Channel channel = connection.createChannel()) {

            // 流控监听：Broker 内存到 high watermark 会 block 生产者（connection.blocked）
            connection.addBlockedListener(
                    reason -> {
                        blocked.set(true);
                        log.warn("[!] Broker 触发流控（BLOCK 生产者）：{} —— 发布速率将出现断崖", reason);
                    },
                    () -> {
                        blocked.set(false);
                        log.info("[i] Broker 解除流控（UNBLOCK），恢复发布");
                    }
            );

            channel.queueDeclare(props.getQueue(), true, false, false, null);

            byte[] body = payload(props.getPayloadBytes());
            int total = props.getMessageCount();
            long start = System.currentTimeMillis();

            // 全速发布：默认交换机 ""，routingKey = 队列名；PERSISTENT_TEXT_PLAIN = 持久化文本
            for (int i = 0; i < total; i++) {
                channel.basicPublish("", props.getQueue(),
                        MessageProperties.PERSISTENT_TEXT_PLAIN, body);
                publishedCount.incrementAndGet();
            }

            long elapsed = System.currentTimeMillis() - start;
            log.info("[x] 生产者完成：共发布 {} 条，耗时 {} ms", total, elapsed);
            return elapsed;
        }
    }

    public long getPublishedCount() {
        return publishedCount.get();
    }

    public boolean isBlocked() {
        return blocked.get();
    }

    /** 构造定长 ASCII 负载，兼容 contentType=text/plain。 */
    private static byte[] payload(int bytes) {
        byte[] b = new byte[Math.max(1, bytes)];
        Arrays.fill(b, (byte) 'x');
        return b;
    }
}

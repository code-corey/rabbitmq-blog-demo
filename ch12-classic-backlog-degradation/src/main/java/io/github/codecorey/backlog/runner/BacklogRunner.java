package io.github.codecorey.backlog.runner;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import io.github.codecorey.backlog.config.BacklogProperties;
import io.github.codecorey.backlog.consumer.SlowConsumer;
import io.github.codecorey.backlog.monitor.QueueMonitor;
import io.github.codecorey.backlog.producer.HighSpeedProducer;
import io.github.codecorey.backlog.support.Connections;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 压测编排：清空队列 → 启动慢消费者 → 启动监控 → 高速灌消息。
 *
 * <p>{@code run()} 返回后由 {@code spring.main.keep-alive} 保活，慢消费者继续慢慢排空队列，
 * 监控线程持续打印队列深度下降，直到深度归零。
 *
 * <p>对应博客《Classic 队列为什么一堆积就变慢》——博客为纯性能分析、无代码，本 module 为自研压测演示。
 */
@Component
public class BacklogRunner implements CommandLineRunner {

    private static final Logger log = LoggerFactory.getLogger(BacklogRunner.class);

    private final BacklogProperties props;
    private final SlowConsumer consumer;
    private final HighSpeedProducer producer;
    private final QueueMonitor monitor;

    public BacklogRunner(BacklogProperties props, SlowConsumer consumer,
                         HighSpeedProducer producer, QueueMonitor monitor) {
        this.props = props;
        this.consumer = consumer;
        this.producer = producer;
        this.monitor = monitor;
    }

    @Override
    public void run(String... args) throws Exception {
        log.info("=== Classic 队列积压压测开始：queue={}, messageCount={}, consumerSleepMs={}ms, payload={}B ===",
                props.getQueue(), props.getMessageCount(), props.getConsumerSleepMs(), props.getPayloadBytes());

        // 1) 可选：清空队列，保证本次压测干净可复现
        if (props.isPurgeOnStart()) {
            try (Connection c = Connections.factory(props).newConnection("backlog-purge");
                 Channel ch = c.createChannel()) {
                ch.queueDeclare(props.getQueue(), true, false, false, null);
                ch.queuePurge(props.getQueue());
                log.info("[*] 已清空队列 {}（purge-on-start=true）", props.getQueue());
            }
        }

        // 2) 启动慢消费者（异步消费，独立连接）
        Connection consumerConn = consumer.start();

        // 3) 启动监控（守护线程）
        AtomicBoolean producerDone = new AtomicBoolean(false);
        AtomicLong producerElapsed = new AtomicLong(-1);
        monitor.start(consumer, producer, producerDone, producerElapsed);

        // 4) 高速灌消息（阻塞直到发完）；期间监控线程会持续打印进度、队列深度与发布速率
        long elapsed = producer.sendAll();
        producerElapsed.set(elapsed);
        producerDone.set(true);

        log.info("=== 生产阶段结束：耗时 {}ms。消费者继续排空，keep-alive 保活，观察队列深度下降（Ctrl+C 退出）===",
                elapsed);

        // consumerConn 不主动关闭：keep-alive 维持进程，消费者继续排空，监控在深度归零后自行退出
    }
}

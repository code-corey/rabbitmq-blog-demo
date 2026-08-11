package io.github.codecorey.backlog.monitor;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import io.github.codecorey.backlog.config.BacklogProperties;
import io.github.codecorey.backlog.consumer.SlowConsumer;
import io.github.codecorey.backlog.producer.HighSpeedProducer;
import io.github.codecorey.backlog.support.Connections;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 队列监控：独立连接，用 {@code queueDeclarePassive}（被动声明）周期采样队列深度，
 * 并打印已发布、已消费、队列深度、近窗口发布速率、流控状态与生产者耗时。
 *
 * <p>被动声明不创建队列、不改队列属性，只回读消息数与消费者数，适合做监控探针。
 * 作为守护线程运行：当生产者已完成、且队列深度归零时自行退出。
 */
@Component
public class QueueMonitor {

    private static final Logger log = LoggerFactory.getLogger(QueueMonitor.class);

    private final BacklogProperties props;

    public QueueMonitor(BacklogProperties props) {
        this.props = props;
    }

    /**
     * 启动守护监控线程。
     *
     * @param consumer       提供已消费计数
     * @param producer       提供已发布计数与流控状态
     * @param producerDone   生产者是否已发完
     * @param producerElapsed 生产者总耗时（生产者发完后写入）
     */
    public void start(SlowConsumer consumer, HighSpeedProducer producer,
                      AtomicBoolean producerDone, AtomicLong producerElapsed) {
        Thread t = new Thread(
                () -> monitor(consumer, producer, producerDone, producerElapsed),
                "backlog-monitor");
        t.setDaemon(true);
        t.start();
    }

    private void monitor(SlowConsumer consumer, HighSpeedProducer producer,
                         AtomicBoolean producerDone, AtomicLong producerElapsed) {
        try (Connection connection = Connections.factory(props).newConnection("backlog-monitor");
             Channel channel = connection.createChannel()) {

            long interval = props.getMonitorIntervalMs();
            long lastPublished = 0;
            long lastTime = System.currentTimeMillis();

            while (true) {
                long depth;
                try {
                    // 被动声明：回读消息数，不修改队列
                    depth = channel.queueDeclarePassive(props.getQueue()).getMessageCount();
                } catch (IOException e) {
                    // channel 级异常会使通道失效，监控退出
                    log.warn("[监] 采样队列深度失败，监控退出：{}", e.getMessage());
                    break;
                }

                long now = System.currentTimeMillis();
                long published = producer.getPublishedCount();
                long consumed = consumer.getConsumedCount();
                long dt = now - lastTime;
                long rate = dt > 0 ? (published - lastPublished) * 1000L / dt : 0;

                log.info("[监] 已发布={}, 已消费={}, 队列深度={}, 近{}ms发布速率≈{}条/s, 流控={}, 生产者耗时={}ms",
                        published, consumed, depth, interval, rate,
                        producer.isBlocked() ? "BLOCKED" : "正常",
                        producerDone.get() ? producerElapsed.get() : -1);

                lastPublished = published;
                lastTime = now;

                // 生产者发完且队列排空 → 监控收工
                if (producerDone.get() && depth == 0) {
                    log.info("[监] 队列已排空（已消费={}），监控退出", consumed);
                    break;
                }
                Thread.sleep(interval);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            log.error("监控线程异常退出", e);
        }
    }
}

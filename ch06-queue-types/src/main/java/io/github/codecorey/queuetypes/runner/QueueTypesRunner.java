package io.github.codecorey.queuetypes.runner;

import io.github.codecorey.queuetypes.config.RabbitMqProperties;
import io.github.codecorey.queuetypes.consumer.ClassicConsumer;
import io.github.codecorey.queuetypes.consumer.QuorumConsumer;
import io.github.codecorey.queuetypes.consumer.StreamConsumer;
import io.github.codecorey.queuetypes.producer.ClassicProducer;
import io.github.codecorey.queuetypes.producer.QuorumProducer;
import io.github.codecorey.queuetypes.producer.StreamProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * 启动就绪后依次演示三种队列类型的声明与收发（对应博客第四节）。
 *
 * <p>三个阶段，每阶段均为「先发送、再消费」：
 * <ol>
 *   <li>Classic —— 默认类型，对比基线（不带 {@code x-queue-type}）。</li>
 *   <li>Quorum —— 仲裁队列，{@code durable=true / exclusive=false} 强制，{@code x-queue-type=quorum}。</li>
 *   <li>Stream —— 流式队列，{@code basicQos} + {@code x-stream-offset} 消费（原生 Channel）。</li>
 * </ol>
 *
 * <p>{@code basicConsume} 是同步 RPC，返回时消费者已在 Broker 注册完成，故随后发送的消息一定会被投递，
 * 无需额外等待。{@code spring.main.keep-alive=true} 保活进程，让异步消费者持续运行。
 *
 * <p><b>关于 Stream offset</b>：默认 {@code first}，配合「先发后消费」可回放已写入的全部消息，
 * 演示 Stream 的 Replay / Time-travel；若改为 {@code last} / {@code next}，因只消费订阅点之后的消息，
 * 建议调整为先订阅后发送，或保持 {@code first} 观察历史回放。
 */
@Component
public class QueueTypesRunner {

    private static final Logger log = LoggerFactory.getLogger(QueueTypesRunner.class);

    private final ClassicProducer classicProducer;
    private final ClassicConsumer classicConsumer;
    private final QuorumProducer quorumProducer;
    private final QuorumConsumer quorumConsumer;
    private final StreamProducer streamProducer;
    private final StreamConsumer streamConsumer;
    private final RabbitMqProperties props;

    public QueueTypesRunner(ClassicProducer classicProducer, ClassicConsumer classicConsumer,
                            QuorumProducer quorumProducer, QuorumConsumer quorumConsumer,
                            StreamProducer streamProducer, StreamConsumer streamConsumer,
                            RabbitMqProperties props) {
        this.classicProducer = classicProducer;
        this.classicConsumer = classicConsumer;
        this.quorumProducer = quorumProducer;
        this.quorumConsumer = quorumConsumer;
        this.streamProducer = streamProducer;
        this.streamConsumer = streamConsumer;
        this.props = props;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void onReady() throws Exception {
        List<String> messages = List.of("queue-types 消息 #1", "queue-types 消息 #2", "queue-types 消息 #3");

        // ===== Phase 1：Classic（默认 FIFO，对比基线）=====
        log.info("======== Phase 1/3：Classic 经典队列（默认类型，对比基线） ========");
        classicProducer.send(messages);
        classicConsumer.start();

        // ===== Phase 2：Quorum（仲裁队列）=====
        log.info("======== Phase 2/3：Quorum 仲裁队列（x-queue-type=quorum，durable/exclusive 强制） ========");
        quorumProducer.send(messages);
        quorumConsumer.start();

        // ===== Phase 3：Stream（流式队列，x-stream-offset 消费）=====
        log.info("======== Phase 3/3：Stream 流式队列（x-queue-type=stream，x-stream-offset={}） ========",
                props.getStreamOffset());
        streamProducer.send(messages);
        streamConsumer.start();

        log.info("======== 三种队列演示已启动，异步消费中（keep-alive 保活），Ctrl+C 退出 ========");
    }
}

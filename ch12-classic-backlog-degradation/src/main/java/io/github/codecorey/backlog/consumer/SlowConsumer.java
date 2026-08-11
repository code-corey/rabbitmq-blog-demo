package io.github.codecorey.backlog.consumer;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import io.github.codecorey.backlog.config.BacklogProperties;
import io.github.codecorey.backlog.support.Connections;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 慢消费者：{@code basicQos(1)} + 每条 {@code Thread.sleep(consumerSleepMs)}，刻意制造积压。
 *
 * <p>独立连接、手动 ACK，让队列只进不出，把队列深度迅速顶到约 2048 条内存窗口之外，
 * 迫使持久化层落盘、读写争用、最终触发流控。已消费数通过 {@link #getConsumedCount()}
 * 暴露给监控线程。
 */
@Component
public class SlowConsumer {

    private static final Logger log = LoggerFactory.getLogger(SlowConsumer.class);

    private final BacklogProperties props;
    private final AtomicLong consumedCount = new AtomicLong();

    public SlowConsumer(BacklogProperties props) {
        this.props = props;
    }

    /**
     * 建立独立连接，声明经典队列（durable），{@code basicQos(1)} 后异步消费。
     *
     * @return 消费者连接，由调用方持有以维持消费线程存活
     */
    public Connection start() throws Exception {
        Connection connection = Connections.factory(props).newConnection("backlog-consumer");
        Channel channel = connection.createChannel();

        // 队列名, durable=true, exclusive=false, autoDelete=false, arguments=null
        channel.queueDeclare(props.getQueue(), true, false, false, null);
        // prefetch=1：消费端能多慢就多慢，让队列只堆不消
        channel.basicQos(1);

        log.info("[*] 慢消费者启动：queue={}, basicQos=1, sleep={}ms/条", props.getQueue(), props.getConsumerSleepMs());

        channel.basicConsume(props.getQueue(), false, new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope,
                                       AMQP.BasicProperties properties, byte[] body)
                    throws IOException {
                try {
                    // 故意睡 consumerSleepMs：把投递节奏压到 ~20 条/s，与高速生产者形成巨大落差
                    Thread.sleep(props.getConsumerSleepMs());
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                channel.basicAck(envelope.getDeliveryTag(), false);
                consumedCount.incrementAndGet();
            }
        });

        return connection;
    }

    public long getConsumedCount() {
        return consumedCount.get();
    }
}

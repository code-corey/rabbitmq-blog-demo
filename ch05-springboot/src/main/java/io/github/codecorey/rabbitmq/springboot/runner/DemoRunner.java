package io.github.codecorey.rabbitmq.springboot.runner;

import io.github.codecorey.rabbitmq.springboot.producer.DemoProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

/**
 * 启动就绪后自动发 3 条消息，触发 DemoConsumer 消费，形成完整收发闭环。
 */
@Component
public class DemoRunner {

    private static final Logger log = LoggerFactory.getLogger(DemoRunner.class);

    private final DemoProducer producer;

    public DemoRunner(DemoProducer producer) {
        this.producer = producer;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void onReady() {
        log.info("=== 启动就绪，发送 3 条演示消息 ===");
        for (int i = 1; i <= 3; i++) {
            producer.send("spring-amqp 消息 #" + i);
        }
    }
}

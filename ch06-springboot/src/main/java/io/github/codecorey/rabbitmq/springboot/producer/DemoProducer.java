package io.github.codecorey.rabbitmq.springboot.producer;

import io.github.codecorey.rabbitmq.springboot.config.RabbitConfig;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.stereotype.Service;

/**
 * 对应博客第四节：用 RabbitTemplate 发送，并注册 Publisher Confirms / Returns 回调。
 */
@Service
public class DemoProducer {

    private static final Logger log = LoggerFactory.getLogger(DemoProducer.class);

    private final RabbitTemplate rabbitTemplate;

    public DemoProducer(RabbitTemplate rabbitTemplate) {
        this.rabbitTemplate = rabbitTemplate;
    }

    public void send(String message) {
        rabbitTemplate.convertAndSend(RabbitConfig.EXCHANGE, RabbitConfig.ROUTING_KEY, message);
        log.info("[x] 已发送: {}", message);
    }

    @PostConstruct
    public void init() {
        // Publisher Confirm：Broker 已接收（correlated 模式下异步回调）
        rabbitTemplate.setConfirmCallback((correlationData, ack, cause) -> {
            if (ack) {
                log.info("Publisher Confirm: ACK");
            } else {
                log.error("Publisher Confirm: NACK, cause={}", cause);
            }
        });
        // Returns：Exchange 收到但无 Queue 可投（不可路由）
        rabbitTemplate.setReturnsCallback(returned -> log.warn(
                "消息不可路由: replyCode={}, replyText={}, exchange={}, routingKey={}",
                returned.getReplyCode(), returned.getReplyText(),
                returned.getExchange(), returned.getRoutingKey()));
    }
}

package io.github.codecorey.rabbitmq.springboot.consumer;

import com.rabbitmq.client.Channel;
import io.github.codecorey.rabbitmq.springboot.config.RabbitConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

import java.io.IOException;

/**
 * 对应博客第五节：@RabbitListener 消费，手动 Ack。
 *
 * <p>acknowledge-mode 配为 manual（见 application.yml），故在此显式 channel.basicAck。
 */
@Component
public class DemoConsumer {

    private static final Logger log = LoggerFactory.getLogger(DemoConsumer.class);

    @RabbitListener(queues = RabbitConfig.QUEUE)
    public void onMessage(String payload, Channel channel,
                          @Header(AmqpHeaders.DELIVERY_TAG) long deliveryTag) throws IOException {
        log.info("收到: {}", payload);
        channel.basicAck(deliveryTag, false);
    }
}

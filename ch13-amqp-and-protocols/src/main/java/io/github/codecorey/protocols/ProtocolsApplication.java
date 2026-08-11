package io.github.codecorey.protocols;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * RabbitMQ 多协议篇入口（对应博客《AMQP 1.0 与多协议——MQTT、STOMP、Stream》）。
 *
 * <p>本 module 只跑一个 {@code AMQP 0-9-1} 基线 demo——用原生 amqp-client 连本机 Broker
 * 收发一条消息、打印协议版本信息，作为「默认协议」基线。MQTT / STOMP / Stream 协议的测试
 * 用命令行客户端（mosquitto_pub / nc）演示，详见 README。
 */
@SpringBootApplication
public class ProtocolsApplication {

    public static void main(String[] args) {
        SpringApplication.run(ProtocolsApplication.class, args);
    }
}

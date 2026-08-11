package io.github.codecorey.sharding;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * 博客《消息分片存储插件 Sharding》示例入口（原生 amqp-client）。
 *
 * <p>默认运行消费者（ShardingConsumer）；加 {@code --app.mode=send} 切换为生产者（ShardingProducer）。
 *
 * <p><b>运行前置</b>：Broker 必须已启用 {@code rabbitmq_sharding} 插件，并在 Admin → Policies 配置好
 * 匹配 {@code sharding_} 前缀的 sharding 策略，否则声明 {@code x-modulus-hash} 交换机会失败。详见 README。
 */
@SpringBootApplication
public class ShardingApplication {

    public static void main(String[] args) {
        SpringApplication.run(ShardingApplication.class, args);
    }
}

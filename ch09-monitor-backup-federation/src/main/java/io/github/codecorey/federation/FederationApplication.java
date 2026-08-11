package io.github.codecorey.federation;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * 博客《RabbitMQ 监控、备份与联邦同步》第 09 篇示例入口（原生 amqp-client）。
 *
 * <p>启动时同时运行：
 * <ul>
 *   <li>{@link io.github.codecorey.federation.consumer.DownStreamConsumer} ——
 *       连<b>下游</b> Broker，声明本地 Exchange/Queue/Binding 并 {@code basicConsume} 监听</li>
 *   <li>{@link io.github.codecorey.federation.producer.UpStreamProducer} ——
 *       向<b>上游</b> Broker 的 {@code fed_exchange} 发若干消息（联邦消息源）</li>
 * </ul>
 *
 * <p>代码能编译即可运行；实跑需双 Broker + federation 插件（见 README 前置）。
 */
@SpringBootApplication
public class FederationApplication {

    public static void main(String[] args) {
        SpringApplication.run(FederationApplication.class, args);
    }
}

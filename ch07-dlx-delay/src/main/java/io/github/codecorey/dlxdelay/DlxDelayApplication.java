package io.github.codecorey.dlxdelay;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * 博客《RabbitMQ 系列 · 第 7 篇：死信队列与延迟队列》示例入口。
 *
 * <p>用 <b>TTL + DLX（死信交换机）</b>实现延迟队列，跑通"订单 30 分钟关单"链路
 * （演示用短 TTL，如 5 秒）。链路：
 * <pre>
 * Producer → delay.exchange → delay.queue（x-message-ttl + x-dead-letter-exchange，无 Consumer）
 *          → TTL 到期成为死信 → process.exchange → process.queue → Consumer 执行关单
 * </pre>
 * 涉及两个交换机（delay.exchange 与 process.exchange），纯队列 arguments 实现，<b>无需插件</b>
 * （区别于博客 5.1 节的 rabbitmq_delayed_message_exchange 插件方案）。
 *
 * <p>启动后由 {@link io.github.codecorey.dlxdelay.runner.DelayQueueRunner}
 * 自动声明拓扑、发送演示消息，并在 process.queue 上消费死信。
 */
@SpringBootApplication
public class DlxDelayApplication {

    public static void main(String[] args) {
        SpringApplication.run(DlxDelayApplication.class, args);
    }
}

package io.github.codecorey.progmodel.runner;

import io.github.codecorey.progmodel.config.RabbitMqProperties;
import io.github.codecorey.progmodel.consumer.PullConsumer;
import io.github.codecorey.progmodel.consumer.PushConsumer;
import io.github.codecorey.progmodel.producer.DemoProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

/**
 * 启动就绪后自动演示原生 amqp-client 七步编程模型（对应博客《基础编程模型》全篇）。
 *
 * <p>演示流程：
 * <ol>
 *   <li>Step 1～4：建连 + 声明 Exchange(带 AE)/Quorum Queue/Binding（由 {@link DemoProducer#open()}）</li>
 *   <li>Step 5：basicPublish 发送 3 条到主队列</li>
 *   <li>【拉模式 basicGet】：主动拉取 3 条 + 手动 ack</li>
 *   <li>【推模式 basicConsume】：注册消费者 → 再发送 2 条 → 服务端推送 + 手动 ack</li>
 *   <li>【备选交换机 alternate-exchange】：发送不可路由消息，从 AE 队列拉回，观察兜底</li>
 *   <li>Step 7：关闭所有连接</li>
 * </ol>
 *
 * <p>{@code spring.main.keep-alive=true} 保证演示期间进程不提前退出（异步推送回调能跑完）；
 * 演示结束后显式 {@code ctx.close()} 让进程优雅退出。
 */
@Component
public class ProgrammingModelDemoRunner {

    private static final Logger log = LoggerFactory.getLogger(ProgrammingModelDemoRunner.class);

    private final DemoProducer producer;
    private final PullConsumer pullConsumer;
    private final PushConsumer pushConsumer;
    private final RabbitMqProperties props;
    private final ConfigurableApplicationContext ctx;

    public ProgrammingModelDemoRunner(DemoProducer producer,
                                      PullConsumer pullConsumer,
                                      PushConsumer pushConsumer,
                                      RabbitMqProperties props,
                                      ConfigurableApplicationContext ctx) {
        this.producer = producer;
        this.pullConsumer = pullConsumer;
        this.pushConsumer = pushConsumer;
        this.props = props;
        this.ctx = ctx;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void onReady() {
        log.info("================ 七步编程模型演示开始 ================");
        try {
            runDemo();
        } catch (Exception e) {
            log.error("演示过程异常: {}", e.getMessage(), e);
        } finally {
            closeQuietly();
            log.info("================ 七步编程模型演示结束 ================");
            // 演示已完成；keep-alive 期间所有异步回调均已跑完，关闭上下文让进程优雅退出
            ctx.close();
        }
    }

    private void runDemo() throws Exception {
        // ---- Step 1～4：建连 + 声明拓扑 ----
        producer.open();
        log.info("Step 1～4: Connection/Channel 已建立，Exchange/Queue/Binding 已声明");

        // ---- Step 5：发送 3 条到主队列（供拉模式消费）----
        log.info("----- Step 5: basicPublish 发送 3 条消息到主队列 -----");
        for (int i = 1; i <= 3; i++) {
            producer.send(props.getRoutingKey(), "pull-" + i, "拉模式测试消息 #" + i);
        }

        // ---- 【拉模式 basicGet】：主动拉取 + 手动 ack ----
        log.info("----- 【拉模式 basicGet】从主队列主动拉取（手动 ack）-----");
        pullConsumer.open();
        int pulled = pullConsumer.pull(props.getQueue(), 3);
        log.info("【拉模式 basicGet】共拉取 {} 条（与 basicConsume 的推送模式对比：拉模式由客户端节奏控制）", pulled);

        // ---- 【推模式 basicConsume】：先注册消费者，再发送 2 条，服务端推送 + 手动 ack ----
        log.info("----- 【推模式 basicConsume】注册消费者 → 再发送 2 条（服务端推送，手动 ack）-----");
        pushConsumer.open();
        pushConsumer.start(props.getQueue());
        for (int i = 1; i <= 2; i++) {
            producer.send(props.getRoutingKey(), "push-" + i, "推模式测试消息 #" + i);
        }
        // 等待异步推送回调执行（手动 ack 在回调内完成）
        Thread.sleep(2000L);
        pushConsumer.stop();

        // ---- 【备选交换机 alternate-exchange】：发送不可路由消息，从 AE 队列拉回 ----
        log.info("----- 【备选交换机 alternate-exchange】发送不可路由消息，观察兜底 -----");
        producer.send(props.getUnroutableKey(), "ae-1", "不可路由消息（应进入 AE 队列）");
        // 等待 Broker 把不可路由消息转到备选交换机、落入 AE 队列
        Thread.sleep(500L);
        int aePulled = pullConsumer.pull(props.getAlternateQueue(), 1);
        log.info("【alternate-exchange】AE 队列拉取 {} 条兜底消息（验证主交换机不可路由时由 AE 接管）", aePulled);

        // ---- Step 7：关闭连接（统一在 finally 兜底，见 closeQuietly）----
        log.info("Step 7: 关闭连接（见 finally）");
    }

    /** Step 7：关闭所有 Producer / Consumer 的连接，忽略单次关闭异常（演示已结束，无需中断流程）。 */
    private void closeQuietly() {
        try {
            pullConsumer.close();
        } catch (Exception e) {
            log.warn("关闭 PullConsumer 时异常: {}", e.getMessage());
        }
        try {
            pushConsumer.close();
        } catch (Exception e) {
            log.warn("关闭 PushConsumer 时异常: {}", e.getMessage());
        }
        try {
            producer.close();
        } catch (Exception e) {
            log.warn("关闭 DemoProducer 时异常: {}", e.getMessage());
        }
    }
}

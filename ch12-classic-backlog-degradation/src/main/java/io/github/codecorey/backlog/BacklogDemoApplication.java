package io.github.codecorey.backlog;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * 第 11 篇配套压测入口：Classic 队列积压演示。
 *
 * <p>启动后高速向经典队列灌持久化消息，慢消费者（basicQos=1 + 每条睡眠）刻意制造积压，
 * 监控线程实时打印队列深度与发布速率，直观呈现
 * 「越过约 2048 条内存窗口 → 落盘 + 随机 I/O + 流控」的断崖。
 *
 * <p>注：博客《Classic 队列为什么一堆积就变慢》原文为纯性能分析、无 Java 代码；本 module 代码为自研演示。
 */
@SpringBootApplication
public class BacklogDemoApplication {

    public static void main(String[] args) {
        SpringApplication.run(BacklogDemoApplication.class, args);
    }
}

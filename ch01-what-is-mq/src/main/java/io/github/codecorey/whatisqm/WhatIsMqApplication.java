package io.github.codecorey.whatisqm;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationEvent;

/**
 * 对应博客《MQ 是什么》第 1.1 节示例入口。
 *
 * <p>纯 Spring Boot 进程内事件 demo，<b>不连接 RabbitMQ</b>。启动流程：
 * <ol>
 *   <li>main 中手动注册 {@link MyApplicationListener}，使其尽早监听到启动过程中的各类 ApplicationEvent；</li>
 *   <li>容器就绪后由 {@link CommandLineRunner#run} 发布一条自定义 ApplicationEvent("myEvent")。</li>
 * </ol>
 * 借此引出 MQ 的核心：把进程内的事件驱动，延伸到跨进程、跨服务的异步消息驱动。
 */
@SpringBootApplication
public class WhatIsMqApplication implements CommandLineRunner {

    private static final Logger log = LoggerFactory.getLogger(WhatIsMqApplication.class);

    private final ApplicationContext applicationContext;

    public WhatIsMqApplication(ApplicationContext applicationContext) {
        this.applicationContext = applicationContext;
    }

    public static void main(String[] args) {
        // 手动注册监听器，确保能捕获到启动早期发布的 ApplicationEvent
        SpringApplication application = new SpringApplication(WhatIsMqApplication.class);
        application.addListeners(new MyApplicationListener());
        application.run(args);
    }

    @Override
    public void run(String... args) {
        log.info("=== 发布自定义事件 myEvent ===");
        // ApplicationEvent 为抽象类，此处用匿名子类实例化（source = "myEvent"）
        applicationContext.publishEvent(new ApplicationEvent("myEvent") {});
    }
}

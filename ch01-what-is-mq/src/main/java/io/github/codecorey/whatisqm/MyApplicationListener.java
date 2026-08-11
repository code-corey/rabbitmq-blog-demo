package io.github.codecorey.whatisqm;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEvent;
import org.springframework.context.ApplicationListener;

/**
 * 对应博客 1.1 节：监听所有 {@link ApplicationEvent}。
 *
 * <p>启动时 Spring Boot 会发布大量 ApplicationEvent（表示启动到了哪一步），这里统一捕获并打印，
 * 让「Producer 发布、Consumer 监听」的消息驱动形态先在进程内直观可见。
 * 博客原文用 System.out.println，此处改用 SLF4J。
 */
public class MyApplicationListener implements ApplicationListener<ApplicationEvent> {

    private static final Logger log = LoggerFactory.getLogger(MyApplicationListener.class);

    @Override
    public void onApplicationEvent(ApplicationEvent applicationEvent) {
        log.info("=====> MyApplicationListener: {}", applicationEvent);
    }
}

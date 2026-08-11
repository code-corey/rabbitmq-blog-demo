package io.github.codecorey.queuetypes;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * 博客《Classic、Quorum、Stream——如何选择队列类型》示例入口。
 *
 * <p>非 Web 应用，启动就绪后由 {@link io.github.codecorey.queuetypes.runner.QueueTypesRunner}
 * 依次演示三种队列的声明与收发：Classic（默认基线）→ Quorum（仲裁队列）→ Stream（流式队列，
 * 用原生 Channel + {@code x-stream-offset} 消费）。{@code spring.main.keep-alive=true} 保活进程，
 * 让异步消费者持续运行。
 */
@SpringBootApplication
public class QueueTypesApplication {

    public static void main(String[] args) {
        SpringApplication.run(QueueTypesApplication.class, args);
    }
}

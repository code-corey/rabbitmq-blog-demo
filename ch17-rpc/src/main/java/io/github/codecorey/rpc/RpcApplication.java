package io.github.codecorey.rpc;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * 博客《RPC 模式——用 RabbitMQ 实现远程调用》示例入口（原生 amqp-client）。
 *
 * <p>启动后由 {@link RpcDemoRunner} 串联：{@link RpcServer} 注册 {@code rpc.queue} 消费者，
 * {@link RpcClient} 用 direct reply-to 发送若干 RPC 请求并阻塞等应答。
 */
@SpringBootApplication
public class RpcApplication {

    public static void main(String[] args) {
        SpringApplication.run(RpcApplication.class, args);
    }
}

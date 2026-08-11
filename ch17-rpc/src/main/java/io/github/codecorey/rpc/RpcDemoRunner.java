package io.github.codecorey.rpc;

import com.rabbitmq.client.ConnectionFactory;
import io.github.codecorey.rpc.config.RabbitMqProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

/**
 * 串联演示博客《RPC 模式》：启动 {@link RpcServer} 消费 {@code rpc.queue}，
 * 再用 {@link RpcClient}（direct reply-to）发若干 RPC 请求，打印请求/应答，
 * 体现 {@code correlationId} 的请求-应答配对。
 *
 * <p>请求混用 {@code fib:N} 与 {@code upper:xxx} 两种类型，并发共用同一条
 * direct reply-to 回复通道，靠 {@code correlationId} 一一对应。连接参数由
 * {@link RabbitMqProperties} + application.yml 注入，默认 localhost:5672/admin/admin/vhost=/。
 */
@Component
public class RpcDemoRunner implements CommandLineRunner {

    private static final Logger log = LoggerFactory.getLogger(RpcDemoRunner.class);

    private final ConnectionFactory connectionFactory;

    public RpcDemoRunner(ConnectionFactory connectionFactory) {
        this.connectionFactory = connectionFactory;
    }

    @Override
    public void run(String... args) throws Exception {
        log.info("========== RabbitMQ RPC 演示（direct reply-to） ==========");

        // 先起 server：注册 rpc.queue 消费者（声明队列、注册完后才发请求）
        try (RpcServer server = new RpcServer(connectionFactory)) {
            server.start();
            // 留一点就绪时间，确保消费者已就位
            Thread.sleep(500);

            // 再起 client：用 direct reply-to 收应答
            try (RpcClient client = new RpcClient(connectionFactory)) {
                // 混用两种请求类型：每条都带唯一 correlationId，共用同一条回复通道
                String[] requests = {
                        "fib:10",              // 期望 55
                        "upper:hello rpc",     // 期望 HELLO RPC
                        "fib:20",              // 期望 6765
                        "upper:direct reply-to", // 期望 DIRECT REPLY-TO
                        "fib:0"                // 期望 0
                };
                for (String request : requests) {
                    String reply = client.call(request);
                    String tag = reply.startsWith("ERROR:") ? "[业务异常]" : "[应答]";
                    log.info("{} 请求={} -> 应答={}", tag, request, reply);
                }
            }
        }

        log.info("========== RPC 演示完成（进程靠 spring.main.keep-alive 保活，Ctrl+C 退出） ==========");
    }
}

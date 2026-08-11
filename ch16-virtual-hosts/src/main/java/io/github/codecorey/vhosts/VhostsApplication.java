package io.github.codecorey.vhosts;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * RabbitMQ Virtual Hosts 示例入口（对应博客《Virtual Hosts——隔离、权限与配额》）。
 *
 * <p>演示：连接到配置指定的 vhost → 声明一个队列 → 收发一条消息 → 打印当前 vhost。
 * 换 vhost 只需改 {@code application.yml} 的 {@code rabbitmq.vhost.virtual-host}，代码一行不动。
 */
@SpringBootApplication
public class VhostsApplication {

    public static void main(String[] args) {
        SpringApplication.run(VhostsApplication.class, args);
    }
}

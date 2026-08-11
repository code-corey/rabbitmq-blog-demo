package io.github.codecorey.backlog.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * 积压压测配置（绑定 application.yml 的 {@code app.*}）。
 *
 * <p>同时承载 RabbitMQ 连接参数与压测参数：连接参数便于切换 Broker，
 * 压测参数便于调整积压规模、消费速率与负载大小，从而直观触发
 * 「内存窗口越线 → 落盘 + 随机 I/O + 流控」的断崖。
 */
@Component
@ConfigurationProperties(prefix = "app")
public class BacklogProperties {

    /** Broker 地址 */
    private String host = "localhost";
    /** Broker 端口 */
    private int port = 5672;
    /** 用户名 */
    private String username = "admin";
    /** 密码 */
    private String password = "admin";
    /** vhost */
    private String virtualHost = "/";
    /** 经典队列名（durable=true） */
    private String queue = "backlog.classic";
    /** 灌入消息总数 */
    private int messageCount = 50000;
    /** 慢消费者每条消息睡眠毫秒，制造积压 */
    private long consumerSleepMs = 50;
    /** 单条消息负载字节数 */
    private int payloadBytes = 256;
    /** 进度 / 队列深度打印间隔（毫秒） */
    private long monitorIntervalMs = 2000;
    /** 启动时是否清空队列，保证可复现 */
    private boolean purgeOnStart = true;

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getVirtualHost() {
        return virtualHost;
    }

    public void setVirtualHost(String virtualHost) {
        this.virtualHost = virtualHost;
    }

    public String getQueue() {
        return queue;
    }

    public void setQueue(String queue) {
        this.queue = queue;
    }

    public int getMessageCount() {
        return messageCount;
    }

    public void setMessageCount(int messageCount) {
        this.messageCount = messageCount;
    }

    public long getConsumerSleepMs() {
        return consumerSleepMs;
    }

    public void setConsumerSleepMs(long consumerSleepMs) {
        this.consumerSleepMs = consumerSleepMs;
    }

    public int getPayloadBytes() {
        return payloadBytes;
    }

    public void setPayloadBytes(int payloadBytes) {
        this.payloadBytes = payloadBytes;
    }

    public long getMonitorIntervalMs() {
        return monitorIntervalMs;
    }

    public void setMonitorIntervalMs(long monitorIntervalMs) {
        this.monitorIntervalMs = monitorIntervalMs;
    }

    public boolean isPurgeOnStart() {
        return purgeOnStart;
    }

    public void setPurgeOnStart(boolean purgeOnStart) {
        this.purgeOnStart = purgeOnStart;
    }
}

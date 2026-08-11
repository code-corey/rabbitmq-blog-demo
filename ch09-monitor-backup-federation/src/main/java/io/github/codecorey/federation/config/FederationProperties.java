package io.github.codecorey.federation.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * 绑定 application.yml 中 {@code app.*} 配置。
 *
 * <p>Federation 联邦拓扑涉及<b>两个 Broker</b>：
 * <ul>
 *   <li><b>upstream</b>（上游 / 消息源）—— {@code UpStreamProducer} 向其 {@code fed_exchange} 发消息</li>
 *   <li><b>downstream</b>（下游 / 消费者本地）—— {@code DownStreamConsumer} 声明本地 Exchange/Queue 并监听；
 *       联邦链路由<b>下游主动</b>在管理控制台配 upstream URI + Policy 后建立，上游消息同步到下游</li>
 * </ul>
 *
 * <p>博客原文拓扑：下游消费者连 {@code 192.168.65.112}（vhost={@code /mirror}），
 * 上游消息源在 {@code 192.168.65.193}（upstream URI：{@code amqp://admin:admin@192.168.65.193:5672/}）。
 *
 * <p>默认两组都指向 {@code localhost:5672}（admin/admin/vhost=/），双 broker 实跑时按机房修改。
 *
 * <p>注意：DownStream 与 UpStream 建议使用<b>相同 Virtual Host</b>。
 */
@Component
@ConfigurationProperties(prefix = "app")
public class FederationProperties {

    /** 上游 Broker（消息源）。 */
    private Broker upstream = new Broker();
    /** 下游 Broker（消费者本地）。 */
    private Broker downstream = new Broker();

    /** 联邦交换机名（上下游同名，上游不存在则联邦自动创建）。 */
    private String exchange = "fed_exchange";
    /** 下游队列名。 */
    private String queue = "fed_queue";
    /** 路由键（博客示例为 routKey）。 */
    private String routingKey = "routKey";

    /** UpStreamProducer 发送条数。 */
    private int producerCount = 5;
    /** UpStreamProducer 首条发送前等待毫秒，留给下游消费者与联邦链路就绪。 */
    private long producerDelayMs = 2000;

    public Broker getUpstream() {
        return upstream;
    }

    public void setUpstream(Broker upstream) {
        this.upstream = upstream;
    }

    public Broker getDownstream() {
        return downstream;
    }

    public void setDownstream(Broker downstream) {
        this.downstream = downstream;
    }

    public String getExchange() {
        return exchange;
    }

    public void setExchange(String exchange) {
        this.exchange = exchange;
    }

    public String getQueue() {
        return queue;
    }

    public void setQueue(String queue) {
        this.queue = queue;
    }

    public String getRoutingKey() {
        return routingKey;
    }

    public void setRoutingKey(String routingKey) {
        this.routingKey = routingKey;
    }

    public int getProducerCount() {
        return producerCount;
    }

    public void setProducerCount(int producerCount) {
        this.producerCount = producerCount;
    }

    public long getProducerDelayMs() {
        return producerDelayMs;
    }

    public void setProducerDelayMs(long producerDelayMs) {
        this.producerDelayMs = producerDelayMs;
    }

    /**
     * 单个 Broker 连接参数（upstream / downstream 各一份）。
     */
    public static class Broker {
        private String host = "localhost";
        private int port = 5672;
        private String username = "admin";
        private String password = "admin";
        private String virtualHost = "/";

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
    }
}

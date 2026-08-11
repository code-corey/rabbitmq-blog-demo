package io.github.codecorey.queuetypes;

import java.util.HashMap;
import java.util.Map;

/**
 * 三种队列的声明参数集中处——Producer 与 Consumer 必须用「同一份」参数声明（博客 4.1：
 * Producer 与 Consumer 声明须一致），抽到一起避免两边漂移。
 *
 * <ul>
 *   <li>Classic：默认类型，不带 {@code x-queue-type}（queueDeclare 传 {@code null} 即可）。</li>
 *   <li>Quorum：{@code x-queue-type=quorum}；且 durable 必须 true、exclusive 必须 false。</li>
 *   <li>Stream：{@code x-queue-type=stream} + {@code x-max-length-bytes} +
 *       {@code x-stream-max-segment-size-bytes}。</li>
 * </ul>
 */
public final class DeclareArguments {

    private DeclareArguments() {
    }

    /** 博客 4.1：Quorum 队列声明参数。 */
    public static Map<String, Object> quorumArgs() {
        Map<String, Object> args = new HashMap<>();
        args.put("x-queue-type", "quorum");
        return args;
    }

    /** 博客 4.2：Stream 队列声明参数（日志容量上限 + 单段大小）。 */
    public static Map<String, Object> streamArgs() {
        Map<String, Object> args = new HashMap<>();
        args.put("x-queue-type", "stream");
        args.put("x-max-length-bytes", 20_000_000_000L);        // 约 20G，博客原文取值
        args.put("x-stream-max-segment-size-bytes", 100_000_000); // 单段约 100M
        return args;
    }

    /**
     * 博客 4.2：Stream 消费参数，携带 {@code x-stream-offset}。
     *
     * @param offset first / last / next / 数字偏移量
     */
    public static Map<String, Object> streamConsumeArgs(String offset) {
        Map<String, Object> args = new HashMap<>();
        args.put("x-stream-offset", offset);
        return args;
    }
}

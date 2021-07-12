package com.hansight.util;

import com.hansight.atom.panther.cfg.Configuration;
import com.hansight.atom.panther.parser.bean.HaliteSubExpression;
import com.hansight.atom.panther.parser.halite.HaliteParser;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPubSub;

import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Stream;

/**
 * Copyright: 瀚思安信（北京）软件技术有限公司，保留所有权利。
 *
 * @author yitian_song
 * @created 2019/11/4
 * @description Halite 解析器
 */
public class HaliteTranslator {
    private static final Logger logger = LoggerFactory.getLogger(IntelligenceGroupUtil.class);

    private static HaliteParser haliteParser = HaliteParser.instance();
    private static Thread subscribeThread;
    private static Subscriber subscriber;
    private static volatile Jedis jedis;

    private static String HALITE_METADATA_KEY = "discover:datasource:alarm:zh_cn";
    private static String HALITE_METADATA_UPDATE_CHANNEL = "discover-datasource";
    private static String HALITE_METADATA_UPDATE_MSG_PREFIX = "alarm:";

    static {
        subscriber = new Subscriber();
        jedis = new Jedis("172.16.100.193", 6379);
        jedis.auth("cloud@hansight.com");
        runSubscribe();
        updateMetadata();
        registerCloseConnection();
    }

    private static void runSubscribe() {
        subscribeThread = new Thread(() -> {
            try {
                jedis.subscribe(subscriber, HALITE_METADATA_UPDATE_CHANNEL);
            } finally {
                jedis.close();
            }
        });
        subscribeThread.setName("Redis subscriber of Halite metadata");
        subscribeThread.setDaemon(true);
        subscribeThread.start();
    }

    private static void registerCloseConnection() {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            if (subscriber != null) {
                subscriber.unsubscribe(HALITE_METADATA_UPDATE_CHANNEL);
            }
        }));
    }

    private static void updateMetadata() {
        try {
            String metadata = jedis.get(HALITE_METADATA_KEY);
            Configuration.instance().registerMetaData(Collections.singletonMap("event", metadata));
        } catch (Exception e) {
            logger.error("Error on load Halite metadata from Redis: ", e);
        }
    }

    public static void main(String[] args) {
        /*String halite = "源地址 belong 内网IP and 目的端口 = 53 and not 目的地址 belong 内网IP and 发送流量 > 1000000 and not 目的地址 belong 保留IP地址列表 and 事件摘要 = \"nta_flow\"";
        System.out.println(translateFilter(halite));
        String relHalite = "A.源地址 = B.源地址 and A.目的地址 = B.目的地址 and A.源端口 = B.源端口 and A.目的端口 = B.目的端口";
        System.out.println(translateWhereCnd(relHalite));*/
        String halite = "((源地址 belong 内网IP and 目的端口 = 53) and (not 目的地址 belong 内网IP and 发送流量 > 1000000)) and 发送流量 > 1000000";
        System.out.println(getPostfixExpressions(halite));
    }

    public static String translateWhereCnd(String expression) {
        if (StringUtils.isBlank(expression)) {
            return null;
        }
        Queue<Object> queue = haliteParser.convertHalite(expression, null);
        List<String> exps = new ArrayList<>();
        String notPrefix = "";
        while (!queue.isEmpty()) {
            Object item = queue.poll();
            if (item == null) {
                continue;
            }
            if (item instanceof HaliteSubExpression) {
                HaliteSubExpression subExpression = (HaliteSubExpression) item;
                switch (subExpression.getOperator().toLowerCase()) {
                    case "=":
                        String eventAliasA = subExpression.getField().substring(0, subExpression.getField().indexOf("."));
                        String fieldNameA = subExpression.getField().substring(eventAliasA.length() + 1);

                        String eventAliasB = subExpression.getValue().substring(0, subExpression.getValue().indexOf("."));
                        String fieldNameB = subExpression.getValue().substring(eventAliasB.length() + 1);
                        exps.add(String.format("%sExpressionUtil.equal(nodeA, \"%s\", nodeB, \"%s\")", notPrefix, fieldNameA, fieldNameB));
                        break;
                    default:
                        exps.add(item.toString());
                }
            } else if (StringUtils.equalsIgnoreCase(item.toString(), "and")) {
                exps.add("&&");
            } else if (StringUtils.equalsIgnoreCase(item.toString(), "or")) {
                exps.add("||");
            } else if (!StringUtils.equalsIgnoreCase(item.toString(), "not")) {
                exps.add(item.toString());
            }
            notPrefix = StringUtils.equalsIgnoreCase(item.toString(), "not") ? "!" : "";
        }
        return String.format("// %s\n%s", expression, String.join(" ", exps));
    }

    public static Queue<Object> getHaliteQueue(String filter) {
        return haliteParser.convertHalite(filter, null);
    }

    public static Stack<Object> getPostfixExpressions(String filter) {
        Queue<Object> queue = getHaliteQueue(filter);
        Stack<Object> temporalStack = new Stack<>();
        Stack<Object> postfixStack = new Stack<>();

        for (Object item : queue) {
            if (item == null) {
                throw new IllegalArgumentException("Invalid halite filter: " + filter);
            }
            if (item instanceof String) {
                switch (item.toString().toLowerCase()) {
                    case "(":
                        temporalStack.push(item);
                        break;
                    case ")":
                        while (!isAnyMatch(temporalStack.peek().toString(), "(")) {
                            postfixStack.push(temporalStack.pop());
                        }
                        temporalStack.pop();
                        break;
                    case "and":
                        while (!temporalStack.empty() && !isAnyMatch(temporalStack.peek().toString(), "(", "or")) {
                            postfixStack.push(temporalStack.pop());
                        }
                        temporalStack.push(item);
                        break;
                    case "or":
                    case "not":
                        while (!temporalStack.empty() && !isAnyMatch(temporalStack.peek().toString(), "(")) {
                            postfixStack.push(temporalStack.pop());
                        }
                        temporalStack.push(item);
                        break;
                    default:
                        throw new IllegalArgumentException("Unsupported operator: " + item);
                }
            } else if (item instanceof HaliteSubExpression) {
                postfixStack.push(item);
            }
        }

        while (!temporalStack.empty()) {
            postfixStack.push(temporalStack.pop());
        }

        return postfixStack;
    }

    private static boolean isAnyMatch(String str, String... matches) {
        return Stream.of(matches).anyMatch(match -> StringUtils.equalsIgnoreCase(str, match));
    }

    private static Queue<Object> reverseQueue(Queue<Object> queue) {
        Stack<Object> stack = new Stack<>();
        queue.forEach(stack::push);
        LinkedBlockingQueue<Object> result = new LinkedBlockingQueue<>();
        while (!stack.isEmpty()) {
            result.offer(stack.pop());
        }
        return result;
    }

    public static String translateFilter(String expression) {
        if (StringUtils.isBlank(expression)) {
            return null;
        }
        Queue<Object> queue = haliteParser.convertHalite(expression, null);
        List<String> exps = new ArrayList<>();
        String notPrefix = "";
        while (!queue.isEmpty()) {
            Object item = queue.poll();
            if (item == null) {
                continue;
            }
            if (item instanceof HaliteSubExpression) {
                HaliteSubExpression subExpression = (HaliteSubExpression) item;
                switch (subExpression.getOperator().toLowerCase()) {
                    case "=":
                        exps.add(String.format("%sExpressionUtil.equal(node, \"%s\", \"%s\")", notPrefix, subExpression.getField(), subExpression.getValue()));
                        break;
                    case "!=":
                        exps.add(String.format("%sExpressionUtil.notEqual(node, \"%s\", \"%s\")", notPrefix, subExpression.getField(), subExpression.getValue()));
                        break;
                    case "like":
                        exps.add(String.format("%sExpressionUtil.like(node, \"%s\", \"%s\")", notPrefix, subExpression.getField(), subExpression.getValue()));
                        break;
                    case "rlike":
                        exps.add(String.format("%sExpressionUtil.rlike(node, \"%s\", \"%s\")", notPrefix, subExpression.getField(), subExpression.getValue()));
                        break;
                    case ">":
                        exps.add(String.format("%sExpressionUtil.gt(node, \"%s\", %s)", notPrefix, subExpression.getField(), subExpression.getValue()));
                        break;
                    case ">=":
                        exps.add(String.format("%sExpressionUtil.gte(node, \"%s\", %s)", notPrefix, subExpression.getField(), subExpression.getValue()));
                        break;
                    case "<":
                        exps.add(String.format("%sExpressionUtil.lt(node, \"%s\", %s)", notPrefix, subExpression.getField(), subExpression.getValue()));
                        break;
                    case "<=":
                        exps.add(String.format("%sExpressionUtil.lte(node, \"%s\", %s)", notPrefix, subExpression.getField(), subExpression.getValue()));
                        break;
                    case "exist":
                        exps.add(String.format("%sExpressionUtil.exist(node, \"%s\")", notPrefix, subExpression.getField()));
                        break;
                    case "not_exist":
                        exps.add(String.format("%sExpressionUtil.notExist(node, \"%s\")", notPrefix, subExpression.getField()));
                        break;
                    case "in":
                        exps.add(String.format("%sExpressionUtil.in(node, \"%s\", \"%s\")", notPrefix, subExpression.getField(), subExpression.getValue()));
                        break;
                    case "belong":
                        exps.add(String.format("%sExpressionUtil.belong(node, \"%s\", \"%s\")", notPrefix, subExpression.getField(), subExpression.getValue()));
                        break;
                    case "contain":
                        exps.add(String.format("%sExpressionUtil.contain(node, \"%s\", \"%s\")", notPrefix, subExpression.getField(), subExpression.getValue()));
                        break;
                    case "match":
                        exps.add(String.format("%sExpressionUtil.match(node, \"%s\")", notPrefix, subExpression.getField()));
                        break;
                    default:
                        exps.add(item.toString());
                }
            } else if (StringUtils.equalsIgnoreCase(item.toString(), "and")) {
                exps.add("&&");
            } else if (StringUtils.equalsIgnoreCase(item.toString(), "or")) {
                exps.add("||");
            } else if (!StringUtils.equalsIgnoreCase(item.toString(), "not")) {
                exps.add(item.toString());
            }
            notPrefix = StringUtils.equalsIgnoreCase(item.toString(), "not") ? "!" : "";
        }
        return String.format("// %s\n%s", expression, String.join(" ", exps));
    }

    private static class Subscriber extends JedisPubSub {
        @Override
        public void onMessage(String channel, String message) {
            logger.info(String.format("subscribe %s %s", channel, message));
            if (message.startsWith(HALITE_METADATA_UPDATE_MSG_PREFIX)) {
                updateMetadata();
            }
        }
    }
}

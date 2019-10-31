package com.hansight;

import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPubSub;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Copyright: 瀚思安信（北京）软件技术有限公司，保留所有权利。
 *
 * @author yitian_song
 * @created 2019/10/28
 * @description 安全信息组 belongs (Redis)
 */
public class IntelligenceGroupUtil {
    private static final Logger logger = LoggerFactory.getLogger(IntelligenceGroupUtil.class);
    private static SecIntelSubscriber subscriber;
    private static volatile Jedis jedis;
    private static final String CHANNEL_INTELLIGENCE_CUD = "intelligencemgmt";
    private static final String RESOURCE_MODULE_INTELLIGENCE = "intelligence";
    private static final String RESOURCE_MODULE_INTELLIGENCE_GROUP = "intelligence_group";
    private static final String ENRICH_ATTR_PREFIX = "enrich;attr;";
    private static final String ENRICH_PREFIX = "enrich;";
    private static final String ENRICH_SET = "enrich";
    private static final String RELOAD_MESSAGE = "reloadIntelligenceGroup";
    private static final String OP_ADD = "add";
    private static final String OP_EDIT = "edit";
    private static final String OP_DELETE = "delete";
    private static final String CONTENT_TIME = "time";
    private static final String CONTENT_NUM = "num";
    private static final String CONTENT_STRING = "string";
    private static final String CONTENT_IP = "ip";
    private static Thread subscribeThread;

    static {
        subscriber = new SecIntelSubscriber();
        jedis = new Jedis("172.16.100.193", 6379);
        jedis.auth("cloud@hansight.com");
        runSubscribe();
        subscriber.reloadAll();
        registerCloseConnection();
    }

    private static void runSubscribe() {
        subscribeThread = new Thread(() -> {
            try {
                jedis.subscribe(subscriber, CHANNEL_INTELLIGENCE_CUD);
            } finally {
                jedis.close();
            }
        });
        subscribeThread.setName("Redis subscriber of intelligence group (flink)");
        subscribeThread.setDaemon(true);
        subscribeThread.start();
    }

    private static void registerCloseConnection() {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            if (subscriber != null) {
                subscriber.unsubscribe(CHANNEL_INTELLIGENCE_CUD);
            }
        }));
    }

    static boolean contains(String groupName, String value) {
        if (value == null) {
            return false;
        }
        return subscriber.contains(groupName, value);
    }

    private static class SecIntelSubscriber extends JedisPubSub {

        private Map<String, Set<ContentMatcher>> contentsOfGroupName = new ConcurrentHashMap<>();
        private Map<String, GroupInfo> groupInfos = new ConcurrentHashMap<>();

        SecIntelSubscriber() {
        }

        private boolean contains(String groupName, String value) {
            Set<ContentMatcher> contents = contentsOfGroupName.get(groupName);
            if (contents != null) {
                return contents.stream().anyMatch(content -> content.match(value));
            }
            return false;
        }

        private synchronized void reloadAll() {
            try {
                Set<String> groupIds = jedis.smembers(ENRICH_SET);
                contentsOfGroupName.clear();
                groupInfos.clear();
                for (String groupId : groupIds) {
                    reloadGroupInfo(groupId);
                    reloadContent(groupId);
                }
            } catch (Exception e) {
                logger.error("Error on load redis enrich set: {}", ENRICH_SET, e);
            }
        }

        private synchronized void reloadGroupInfo(String groupId) {
            List<String> info = jedis.hmget(ENRICH_PREFIX + groupId, "name", "type", "intelligenceType");
            groupInfos.put(groupId, GroupInfo.from(info));
        }

        private synchronized void reloadGroup(String... ids) {
            for (String groupId : ids) {
                String originGroupName = groupInfos.get(groupId).getName();
                reloadGroupInfo(groupId);
                String groupName = groupInfos.get(groupId).getName();
                if (!StringUtils.equals(originGroupName, groupName)) {
                    contentsOfGroupName.put(groupName, contentsOfGroupName.remove(originGroupName));
                }
            }
        }

        private synchronized void addGroup(String... ids) {
            for (String groupId : ids) {
                reloadGroupInfo(groupId);
            }
        }

        private synchronized void deleteGroup(String... ids) {
            for (String groupId : ids) {
                String groupName = groupInfos.get(groupId).getName();
                if (groupName != null) {
                    groupInfos.remove(groupId);
                    contentsOfGroupName.remove(groupName);
                }
            }
        }

        private synchronized void reloadContent(String... ids) {
            for (String id : ids) {
                String key = ENRICH_ATTR_PREFIX + id;
                try {
                    Set<String> elements = jedis.smembers(key);
                    GroupInfo info = groupInfos.get(id);
                    String groupName = info.getName();
                    Set<ContentMatcher> matchers = elements.stream()
                            .map(content -> ContentMatcher.of(info, content))
                            .collect(Collectors.toSet());
                    contentsOfGroupName.put(groupName, matchers);
                } catch (Exception e) {
                    logger.error("Error on reload redis enrich set: {}", key, e);
                }
            }
        }

        @Override
        public void onMessage(String channel, String message) {
            logger.info(String.format("subscribe %s %s", channel, message));

            String[] params = message.split(";");
            if (params.length != 3 && !StringUtils.equals(message, RELOAD_MESSAGE)) {
                throw new IllegalArgumentException("Unexpected message format: " + message);
            }
            // Case 1: reload all
            if (StringUtils.equals(message, RELOAD_MESSAGE)) {
                reloadAll();
                return;
            }

            // Case 2: CUD
            String module = params[0];
            String[] intelligenceGroupIds = Arrays.stream(params[1].split(","))
                    .map(String::trim)
                    .distinct()
                    .toArray(String[]::new);
            String operator = params[2];

            if (StringUtils.equals(module, RESOURCE_MODULE_INTELLIGENCE)) {
                switch (operator) {
                    case OP_EDIT:
                        reloadContent(intelligenceGroupIds);
                        break;
                    case OP_ADD:
                    case OP_DELETE:
                    default:
                        throw new NotImplementedException("Message operator " + message + " not implemented");

                }
            } else if (StringUtils.equals(module, RESOURCE_MODULE_INTELLIGENCE_GROUP)) {
                switch (operator) {
                    case OP_EDIT:
                        reloadGroup(intelligenceGroupIds);
                        break;
                    case OP_ADD:
                        addGroup(intelligenceGroupIds);
                        break;
                    case OP_DELETE:
                        deleteGroup(intelligenceGroupIds);
                        break;
                    default:
                        throw new NotImplementedException("Message operator " + message + " not implemented");
                }
            } else {
                throw new NotImplementedException("Message operator " + message + " not implemented");
            }
        }
    }

    private static class GroupInfo {
        private String name;
        private String type;
        private String intelligenceType;

        GroupInfo(String name, String type, String intelligenceType) {
            this.name = name;
            this.type = type;
            this.intelligenceType = intelligenceType;
        }

        static GroupInfo from(List<String> info) {
            String name = info.get(0);
            String type = info.get(1);
            String intelligenceType = info.get(2);
            return new GroupInfo(name, type, intelligenceType);
        }

        String getName() {
            return name;
        }

        String getType() {
            return type;
        }

        String getIntelligenceType() {
            return intelligenceType;
        }

        @Override
        public String toString() {
            return String.format("[%s, %s, %s]", name, type, intelligenceType);
        }
    }

    private abstract static class ContentMatcher {

        String content;

        /*
            1. string, hash  : 正则部分匹配
            2. string, url   : 正则完全匹配
            3. string, email : 字符串比较
            4. ip, ip        : IP比较、IP端比较、CIDR比较
            5. num, port     : 端口比较
            6. time, relative: 相对时间比较
            7. time, absolute: 绝对时间比较
         */
        static ContentMatcher of(GroupInfo info, String content) {
            switch (info.getType()) {
                case CONTENT_TIME:
                    return new TimeMatcher(content);

                case CONTENT_NUM:
                    return new NormalMatcher(content);

                case CONTENT_STRING:
                    if (StringUtils.equals(info.getIntelligenceType(), "hash")) {
                        return new PartialRegexpMatcher(content);
                    }
                    if (StringUtils.equals(info.getIntelligenceType(), "email")) {
                        return new NormalMatcher(content);
                    }
                    if (StringUtils.equals(info.getIntelligenceType(), "url")) {
                        return new FullRegexpMatcher(content);
                    }
                    throw new NotImplementedException("Unexpected intelligence group: " + info);

                case CONTENT_IP:
                    return new IpRangeMatcher(content);

                default:
                    throw new NotImplementedException("Unexpected intelligence group: " + info);
            }
        }


        abstract boolean match(String value);
    }

    private static class TimeMatcher extends ContentMatcher {

        TimeMatcher(String content) {
            this.content = content;
        }

        @Override
        boolean match(String value) {
            // todo
            return StringUtils.equals(content, value);
        }
    }

    private static class NormalMatcher extends ContentMatcher {

        NormalMatcher(String content) {
            this.content = content;
        }

        @Override
        boolean match(String value) {
            return StringUtils.equals(content, value);
        }
    }

    private static class FullRegexpMatcher extends ContentMatcher {

        FullRegexpMatcher(String content) {
            this.content = content;
        }

        @Override
        boolean match(String value) {
            // todo
            return StringUtils.equals(content, value);
        }
    }


    private static class PartialRegexpMatcher extends ContentMatcher {

        PartialRegexpMatcher(String content) {
            this.content = content;
        }

        @Override
        boolean match(String value) {
            // todo
            return StringUtils.equals(content, value);
        }
    }

    private static class IpRangeMatcher extends ContentMatcher {

        IpRangeMatcher(String content) {
            this.content = content;
        }

        @Override
        boolean match(String value) {
            if (content.contains("-")) {
                // 0.0.0.0-255.255.255.255
                String[] bounds = content.split("-");
                long ipAsNum = toNumber(value);
                long upperBound = toNumber(bounds[0]);
                long lowerBound = toNumber(bounds[1]);
                return ipAsNum >= Long.min(upperBound, lowerBound) && ipAsNum <= Long.max(upperBound, lowerBound);
            } else if (content.contains("/")) {
                // CIDR 208.130.29.0/24
                String[] ipAndMask = content.split("/");
                long ipAsNum = toNumber(value);
                long num = toNumber(ipAndMask[0]);
                String mask = ipAndMask[1];
                return ipAsNum >= getLowerBound(num, mask) && ipAsNum <= getUpperBound(num, mask);
            } else {
                return StringUtils.equals(content, value);
            }
        }

        private static long toNumber(String ip) {
            ip = ip.trim();
            String[] ips = ip.split("\\.");
            long num = 0L;
            try {
                for (String i : ips) {
                    num = num << 8 | Integer.parseInt(i);
                }
            } catch (NumberFormatException e) {
                logger.debug("Error on parse ip to number: {}", ip, e);
                return 0L;
            }
            return num;
        }

        private static long getLowerBound(long ip, String mask) {
            try {
                int bits = Integer.parseInt(mask);
                return ip & (0xFFFFFFFFL << (32-bits));
            } catch (NumberFormatException e) {
                logger.debug("Error on parse CIDR mask: {}", mask, e);
                return 0L;
            }
        }

        private static long getUpperBound(long ip, String mask) {
            try {
                int bits = Integer.parseInt(mask);
                return ip | (0xFFFFFFFFL >> bits);
            } catch (NumberFormatException e) {
                logger.debug("Error on parse CIDR mask: {}", mask, e);
                return 0L;
            }
        }
    }
}

package com.hansight;

import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPubSub;

import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Copyright: 瀚思安信（北京）软件技术有限公司，保留所有权利。
 *
 * @author yitian_song
 * @created 2019/10/28
 * @description Redis
 */
public class RedisUtil {
    private static final Logger logger = LoggerFactory.getLogger(RedisUtil.class);
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

    static {
        subscriber = new SecIntelSubscriber();
        jedis = new Jedis("172.16.100.193");
        subscriber.reloadAll();
        jedis.subscribe(subscriber, CHANNEL_INTELLIGENCE_CUD);
    }

    public static Set<String> getIntelligence(String groupId) {
        return subscriber.get(groupId);
    }

    private static class SecIntelSubscriber extends JedisPubSub {

        private Map<String, Set<String>> contentsOfGroupName = new ConcurrentHashMap<>();
        private Map<String, String> groupIdNameMap = new ConcurrentHashMap<>();

        SecIntelSubscriber() {
        }

        private Set<String> get(String groupId) {
            if (groupIdNameMap.containsKey(groupId)) {
                return contentsOfGroupName.get(groupIdNameMap.get(groupId));
            }
            return null;
        }

        private synchronized void reloadAll() {
            try {
                Set<String> groupIds = jedis.smembers(ENRICH_SET);
                contentsOfGroupName.clear();
                groupIdNameMap.clear();
                for (String groupId : groupIds) {
                    reloadNameIdMap(groupId);
                    reloadContent(groupId);
                }
            } catch (Exception e) {
                logger.error("Error on load redis enrich set: {}", ENRICH_SET, e);
            }
        }

        private synchronized void reloadNameIdMap(String groupId) {
            String name = jedis.hget(ENRICH_PREFIX + groupId, "name");
            groupIdNameMap.put(groupId, name);
        }

        private synchronized void reloadGroup(String... ids) {
            for (String groupId : ids) {
                String originGroupName = groupIdNameMap.get(groupId);
                reloadNameIdMap(groupId);
                String groupName = groupIdNameMap.get(groupId);
                if (!StringUtils.equals(originGroupName, groupName)) {
                    contentsOfGroupName.put(groupName, contentsOfGroupName.remove(originGroupName));
                }
            }
        }

        private synchronized void addGroup(String... ids) {
            for (String groupId : ids) {
                reloadNameIdMap(groupId);
            }
        }

        private synchronized void deleteGroup(String... ids) {
            for (String groupId : ids) {
                String groupName = groupIdNameMap.get(groupId);
                if (groupName != null) {
                    groupIdNameMap.remove(groupId);
                    contentsOfGroupName.remove(groupName);
                }
            }
        }

        private synchronized void reloadContent(String... ids) {
            for (String id : ids) {
                String key = ENRICH_ATTR_PREFIX + id;
                try {
                    Set<String> elements = jedis.smembers(key);
                    String groupName = groupIdNameMap.get(id);
                    contentsOfGroupName.put(groupName, elements);
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
}

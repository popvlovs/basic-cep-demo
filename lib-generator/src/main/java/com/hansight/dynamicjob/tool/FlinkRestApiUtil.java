package com.hansight.dynamicjob.tool;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONException;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * Copyright: 瀚思安信（北京）软件技术有限公司，保留所有权利。
 *
 * @author yitian_song
 * @created 2019/10/17
 * @description 负责调用Flink集群的Rest API
 */
public class FlinkRestApiUtil {

    private static final Logger logger = LoggerFactory.getLogger(FlinkRestApiUtil.class);

    public static String savepointAsync(String url, String jobId, String targetDir, boolean isCancelJob) throws Exception {
        String path = String.format("/jobs/%s/savepoints", jobId);

        JSONObject body = new JSONObject();
        body.put("target-directory", targetDir);
        body.put("cancel-job", isCancelJob);
        String response = HttpUtil.doPost(url, path, Collections.emptyList(), body.toJSONString());
        try {
            JSONObject responseJo = JSONObject.parseObject(response);
            String requestId = responseJo.getString("request-id");
            if (StringUtils.isNotEmpty(requestId)) {
                return requestId;
            } else {
                throw new RuntimeException("Error on parse POST savepoint response: " + response);
            }
        } catch (JSONException e) {
            throw new RuntimeException("Error on parse POST savepoint response: " + response, e);
        }
    }

    public static String waitSavepointComplete(String url, String jobId, String triggerId, long timeoutMillis) throws Exception {
        String path = String.format("/jobs/%s/savepoints/%s", jobId, triggerId);
        long startTime = System.currentTimeMillis();
        while (true) {
            String response = HttpUtil.doGet(url, path, Collections.emptyList());
            JSONObject responseJo = JSONObject.parseObject(response);
            JSONObject operationJo = Optional.ofNullable(responseJo.getJSONObject("operation")).orElseGet(JSONObject::new);
            if (operationJo.containsKey("failure-cause")) {
                JSONObject failureJo = operationJo.getJSONObject("failure-cause");
                throw new RuntimeException("Savepoint request failure: " + failureJo.getString("stack-trace"));
            }
            JSONObject statusJo = responseJo.getJSONObject("status");
            String status = statusJo.getString("id");
            if (Objects.equals(status, "COMPLETED")) {
                return response;
            }
            Thread.sleep(TimeUnit.SECONDS.toMillis(1));
            if (System.currentTimeMillis() - startTime > timeoutMillis) {
                throw new TimeoutException("Timeout on waiting savepoint complete");
            }
        }
    }

    public static String getCompleteCheckpointRetry(String url, String jobId, int retryTimes) throws InterruptedException {
        int retry = 0;
        Exception lastException = null;
        while (retry++ <= retryTimes) {
            try {
                return getCompleteCheckpoint(url, jobId);
            } catch (Exception e) {
                lastException = e;
                Thread.sleep(TimeUnit.SECONDS.toMillis(1));
            }
        }
        throw new RuntimeException("Retry exceed time limit " + retryTimes, lastException);
    }

    private static String getCompleteCheckpoint(String url, String jobId) throws Exception {
        String path = String.format("/jobs/%s/checkpoints", jobId);
        String response = HttpUtil.doGet(url, path, Collections.emptyList());
        try {
            JSONObject responseJo = JSONObject.parseObject(response);
            JSONObject latestJo = responseJo.getJSONObject("latest");
            JSONObject savepointJo = latestJo.getJSONObject("savepoint");
            if (!StringUtils.equals(savepointJo.getString("status"), "COMPLETED")) {
                throw new RuntimeException("Get completed savepoint failed");
            }
            return savepointJo.getString("external_path");
        } catch (Exception e) {
            throw new RuntimeException("Error on get completed checkpoint: " + response, e);
        }
    }

    public static String uploadJar(String url, File jarFile) throws Exception {
        String path = "/jars/upload";
        String response = HttpUtil.uploadJar(url, path, jarFile);
        JSONObject responseJo = JSONObject.parseObject(response);
        String filename = responseJo.getString("filename");
        String jarId = filename.substring(filename.lastIndexOf("/") + 1);
        return jarId;
    }

    public static String runJar(String url, String jarId, String entryClass, int parallelism, String savepoint) throws Exception {
        String path = String.format("/jars/%s/run", jarId);
        JSONObject body = new JSONObject();
        body.put("entryClass", entryClass);
        body.put("parallelism", parallelism);
        body.put("savepointPath", savepoint);
        body.put("allowNonRestoredState", true);
        String response = HttpUtil.doPost(url, path, Collections.emptyList(), body.toJSONString());
        JSONObject responseJo = JSONObject.parseObject(response);
        return responseJo.getString("jobid");
    }

    public static List<JSONObject> getUploadedJars(String url, Predicate<String> nameMatch) throws Exception {
        String path = String.format("/jars");
        String response = HttpUtil.doGet(url, path, Collections.emptyList());
        JSONObject responseJo = JSONObject.parseObject(response);
        JSONArray files = responseJo.getJSONArray("files");
        Comparator<? super JSONObject> comparator = Comparator.comparing(file -> file.getLongValue("uploaded"));
        List<JSONObject> sortedFiles = files.stream()
                .map(item -> (JSONObject) item)
                .filter(item -> nameMatch.test(item.getString("name")))
                .sorted(comparator.reversed())
                .collect(Collectors.toList());
        return sortedFiles;
    }

    public static boolean deleteJar(String url, String jarId) {
        try {
            String path = String.format("/jars/%s", jarId);
            HttpUtil.doDelete(url, path, Collections.emptyList());
            return true;
        } catch (Exception e) {
            logger.error("Error on delete Jar: ", e);
            return false;
        }
    }
}

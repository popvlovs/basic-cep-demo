package com.hansight.dynamicjob.tool;

import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.NameValuePair;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.*;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import java.io.File;
import java.io.FileInputStream;
import java.net.URI;
import java.nio.charset.Charset;
import java.util.List;

/**
 * Copyright: 瀚思安信（北京）软件技术有限公司，保留所有权利。
 *
 * @author yitian_song
 * @created 2019/10/17
 * @description .
 */
public class HttpUtil {

    public static String doGet(String url, String path, List<NameValuePair> urlParams) throws Exception {

        URIBuilder uriBuilder = new URIBuilder(url);
        uriBuilder.setPath(path);
        uriBuilder.addParameters(urlParams);
        URI uri = uriBuilder.build();

        RequestConfig.Builder builder = RequestConfig.custom();
        RequestConfig requestConfig = builder
                .setConnectionRequestTimeout(30000)
                .setConnectTimeout(30000)
                .setSocketTimeout(30000).build();
        CloseableHttpClient httpClient = HttpClients.custom().setDefaultRequestConfig(requestConfig).build();
        HttpGet httpGet = new HttpGet(uri);
        HttpResponse response = httpClient.execute(httpGet);

        int statusCode = response.getStatusLine().getStatusCode();
        if (statusCode == HttpStatus.SC_OK || statusCode == HttpStatus.SC_ACCEPTED) {
            return EntityUtils.toString(response.getEntity(), Charset.forName("UTF-8"));
        } else {
            throw new RuntimeException("status-code:" + statusCode + ", error-msg: " + EntityUtils.toString(response.getEntity()));
        }
    }

    public static String uploadJar(String url, String path, File jarFile) throws Exception {

        URIBuilder uriBuilder = new URIBuilder(url);
        uriBuilder.setPath(path);
        URI uri = uriBuilder.build();

        RequestConfig.Builder builder = RequestConfig.custom();
        RequestConfig requestConfig = builder
                .setConnectionRequestTimeout(30000)
                .setConnectTimeout(30000)
                .setSocketTimeout(30000).build();
        CloseableHttpClient httpClient = HttpClients.custom().setDefaultRequestConfig(requestConfig).build();
        HttpPost httpPost = new HttpPost(uri);
        //httpPost.setHeader("Content-Type", "application/x-java-archive");

        MultipartEntityBuilder entityBuilder = MultipartEntityBuilder.create();
        entityBuilder.addBinaryBody("jarfile", jarFile, ContentType.APPLICATION_OCTET_STREAM, jarFile.getName());
        httpPost.setEntity(entityBuilder.build());
        HttpResponse response = httpClient.execute(httpPost);

        int statusCode = response.getStatusLine().getStatusCode();
        if (statusCode == HttpStatus.SC_OK || statusCode == HttpStatus.SC_ACCEPTED) {
            return EntityUtils.toString(response.getEntity(), Charset.forName("UTF-8"));
        } else {
            throw new RuntimeException("status-code:" + statusCode + ", error-msg: " + EntityUtils.toString(response.getEntity()));
        }
    }

    public static String doPost(String url, String path, List<NameValuePair> urlParams, String body) throws Exception {

        URIBuilder uriBuilder = new URIBuilder(url);
        uriBuilder.setPath(path);
        uriBuilder.addParameters(urlParams);
        URI uri = uriBuilder.build();

        RequestConfig.Builder builder = RequestConfig.custom();
        RequestConfig requestConfig = builder
                .setConnectionRequestTimeout(30000)
                .setConnectTimeout(30000)
                .setSocketTimeout(30000).build();
        CloseableHttpClient httpClient = HttpClients.custom().setDefaultRequestConfig(requestConfig).build();
        HttpPost httpPost = new HttpPost(uri);
        httpPost.setHeader("Content-Type", "application/json");
        StringEntity entity = new StringEntity(body, "UTF-8");
        httpPost.setEntity(entity);
        HttpResponse response = httpClient.execute(httpPost);

        int statusCode = response.getStatusLine().getStatusCode();
        if (statusCode == HttpStatus.SC_OK || statusCode == HttpStatus.SC_ACCEPTED) {
            return EntityUtils.toString(response.getEntity(), Charset.forName("UTF-8"));
        } else {
            throw new RuntimeException("status-code:" + statusCode + ", error-msg: " + EntityUtils.toString(response.getEntity()));
        }
    }

    public static String doDelete(String url, String path, List<NameValuePair> urlParams) throws Exception {
        URIBuilder uriBuilder = new URIBuilder(url);
        uriBuilder.setPath(path);
        uriBuilder.addParameters(urlParams);
        URI uri = uriBuilder.build();

        RequestConfig.Builder builder = RequestConfig.custom();
        RequestConfig requestConfig = builder
                .setConnectionRequestTimeout(30000)
                .setConnectTimeout(30000)
                .setSocketTimeout(30000).build();
        CloseableHttpClient httpClient = HttpClients.custom().setDefaultRequestConfig(requestConfig).build();
        HttpDelete httpDelete = new HttpDelete(uri);
        HttpResponse response = httpClient.execute(httpDelete);

        int statusCode = response.getStatusLine().getStatusCode();
        if (statusCode == HttpStatus.SC_OK || statusCode == HttpStatus.SC_ACCEPTED) {
            return EntityUtils.toString(response.getEntity(), Charset.forName("UTF-8"));
        } else {
            throw new RuntimeException("status-code:" + statusCode + ", error-msg: " + EntityUtils.toString(response.getEntity()));
        }
    }
}

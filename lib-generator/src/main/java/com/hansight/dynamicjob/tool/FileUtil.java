package com.hansight.dynamicjob.tool;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URL;
import java.util.jar.JarFile;

/**
 * Copyright: 瀚思安信（北京）软件技术有限公司，保留所有权利。
 *
 * @author yitian_song
 * @created 2019/10/16
 * @description .
 */
public class FileUtil {

    private static final Logger logger = LoggerFactory.getLogger(FileUtil.class);

    public static String read(String path) throws Exception {
        // Try file://
        File target = new File(path);
        if (target.exists() && target.isFile()) {
            return FileUtil.readToString(new FileInputStream(target));
        }

        // Try classpath://
        return FileUtil.readResourceToString(path);
    }

    public static String readToString(InputStream stream) throws IOException {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

        String encoding = "UTF-8";
        byte[] buffer = new byte[1024];

        int len;
        while ((len = stream.read(buffer)) > 0) {
            outputStream.write(buffer, 0, len);
        }
        try {
            return outputStream.toString(encoding);
        } catch (UnsupportedEncodingException e) {
            logger.error("不支持的编码格式：", encoding);
        }
        return null;
    }

    public static String readResourceToString(String path) throws Exception {
        ClassLoader classLoader = FileUtil.class.getClassLoader();
        URL target = classLoader.getResource(path);
        if (target != null) {
            InputStream stream = classLoader.getResourceAsStream(path);
            return readToString(stream);
        }
        throw new RuntimeException("Can not find resource: " + path);
    }

    public static String readJarManifest(URL jarUrl, String entry) {
        try {
            JarFile jar = new JarFile(jarUrl.getFile());
            return jar.getManifest().getMainAttributes().getValue(entry);
        } catch (IOException e) {
            throw new RuntimeException("Error on read Jar file: " + jarUrl.getFile(), e);
        }
    }
}

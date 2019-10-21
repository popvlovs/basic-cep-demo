package com.hansight.dynamicjob.tool;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;


/**
 * 通过Runtime接口调用本地shell命令
 *
 * @author liubo
 */
public class LocalShellExecutor {

    private static final Logger log = LoggerFactory.getLogger(LocalShellExecutor.class);
    public static final String LINE_SEPARATOR = "\n";

    /**
     * 执行shell命令,返回控制台打印结果字符串
     *
     * @param cmd 调用的命令,例如:ipconfig
     * @return ExecutorResult  控制台打印结果字符串
     * @author liubo
     */
    public static ExecutorResult executeReturnString(String cmd, String charset) {

        StringBuffer result = new StringBuffer();
        StringBuffer error = new StringBuffer();
        Process process = null;
        BufferedReader reader = null;
        BufferedReader errReader = null;
        int exitValue = 0;
        try {
            process = getProcess(cmd);
            reader = new BufferedReader(new InputStreamReader(process.getInputStream(), charset));

            // read standard output
            String line;
            while ((line = reader.readLine()) != null) {
                result.append(line + LINE_SEPARATOR);
            }

            // read error output
            errReader = new BufferedReader(new InputStreamReader(process.getErrorStream(), charset));
            while ((line = errReader.readLine()) != null) {
                error.append(line + LINE_SEPARATOR);
            }

            process.getOutputStream().flush();//刷新标准输出流
            process.getOutputStream().close();//关闭标准输出流
            process.waitFor();
        } catch (IOException e) {
            log.error("command error", e);
        } catch (InterruptedException e) {
            log.error("command error", e);
        } finally {
            IOUtils.closeQuietly(reader);
            if (process != null) {
                exitValue = process.exitValue();
                try {
                    process.destroy();
                } catch (Exception e) {

                }
            }
        }
        return new ExecutorResult(result.toString(), error.toString(), exitValue);
    }

    public static class ExecutorResult {
        private String result;
        private String error;
        private int exitValue;

        public ExecutorResult(String result, String error, int exitValue) {
            this.result = result;
            this.error = error;
            this.exitValue = exitValue;
        }

        public String getResult() {
            return result;
        }

        public String getError() {
            return error;
        }

        public boolean hasError() {
            return !StringUtils.isEmpty(error);
        }

        public int getExitValue() {
            return exitValue;
        }
    }


    /**
     * 构造子进程
     *
     * @param cmd 调用的命令,例如:killall -9 java
     * @return Process
     * @throws IOException
     * @author liubo
     */
    private static Process getProcess(String cmd) throws IOException {
        if (System.getProperty("os.name").toLowerCase().indexOf("window") != -1) {
            // windows
            return Runtime.getRuntime().exec(new String[]{"cmd", "/c", cmd});
        } else {
            // linux
            return Runtime.getRuntime().exec(new String[]{"/bin/sh", "-c", cmd});
        }
    }
}
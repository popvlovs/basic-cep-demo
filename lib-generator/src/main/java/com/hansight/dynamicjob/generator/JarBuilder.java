package com.hansight.dynamicjob.generator;

import com.hansight.dynamicjob.tool.FileUtil;
import com.hansight.dynamicjob.tool.LocalShellExecutor;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Arrays;
import java.util.Optional;

/**
 * Copyright: 瀚思安信（北京）软件技术有限公司，保留所有权利。
 *
 * @author yitian_song
 * @created 2019/10/16
 * @description 动态构建Jar包
 */
public class JarBuilder {

    private File sourceJar;
    private File dependencyJarDir;
    private File workspace;
    private File outputDir;
    private File buildClassDir;
    private String mainClassToCompile;
    private String mainClass;
    private boolean purgeAfterBuild = false;

    private JarBuilder() {
    }

    public static JarBuilder builder() {
        JarBuilder builder = new JarBuilder();
        return builder;
    }

    public JarBuilder sourceJar(String sourceJar) throws Exception {
        // Try file://
        File jarFile = new File(sourceJar);
        if (jarFile.exists()) {
            return sourceJar(jarFile);
        }
        // Try classpath://
        ClassLoader classLoader = JarBuilder.class.getClassLoader();
        URL jarResource = classLoader.getResource(sourceJar);
        if (jarResource != null) {
            InputStream sourceJarStream = classLoader.getResourceAsStream(sourceJar);
            if (workspace == null) {
                throw new RuntimeException("Please set workspace before source Jar");
            }
            File sourceJarTmpDir = new File(workspace.getCanonicalFile() + File.separator + "jarTmp");
            checkDir(sourceJarTmpDir);
            File sourceJarTmpFile = new File(sourceJarTmpDir.getCanonicalFile() + File.separator + sourceJar);
            FileUtils.copyInputStreamToFile(sourceJarStream, sourceJarTmpFile);
            return sourceJar(sourceJarTmpFile);
        }
        throw new RuntimeException("Invalid Jar source: " + sourceJar);
    }

    public JarBuilder sourceJar(File sourceJar) throws IOException {
        this.sourceJar = sourceJar;
        if (this.sourceJar == null) {
            throw new NullPointerException("Empty source Jar file");
        }
        if (!this.sourceJar.exists()) {
            String path = this.sourceJar.getPath();
            throw new FileNotFoundException("Jar file not found: " + path);
        }
        return this;
    }

    public JarBuilder dependencyJarDir(String dependencyJarDir) throws IOException {
        // Try file://
        File jarFile = new File(dependencyJarDir);
        if (jarFile.exists()) {
            return dependencyJarDir(dependencyJarDir);
        }
        // Try classpath://
        ClassLoader classLoader = JarBuilder.class.getClassLoader();
        URL dependencyJarDirUrl = classLoader.getResource(dependencyJarDir);
        if (dependencyJarDirUrl != null) {
            InputStream stream = classLoader.getResourceAsStream(dependencyJarDir);
            if (workspace == null) {
                throw new RuntimeException("Please set workspace before source Jar");
            }
            File tmpDir = new File(workspace.getCanonicalFile() + File.separator + "jarTmp");
            checkDir(tmpDir);
            File sourceJarTmpFile = new File(tmpDir.getCanonicalFile() + File.separator + dependencyJarDir);
            FileUtils.copyInputStreamToFile(stream, sourceJarTmpFile);
            return dependencyJarDir(sourceJarTmpFile);
        }
        throw new RuntimeException("Invalid dependencies direction: " + dependencyJarDir);
    }

    public JarBuilder dependencyJarDir(File dependencyJarDir) throws IOException {
        this.dependencyJarDir = dependencyJarDir;
        checkDir(dependencyJarDir, false);
        return this;
    }

    public JarBuilder workspace(String workspace) throws IOException {
        return workspace(new File(workspace));
    }

    public JarBuilder workspace(File workspace) throws IOException {
        this.workspace = workspace;
        checkDir(workspace);
        return this;
    }

    public JarBuilder outputDir(String outputDir) throws IOException {
        return outputDir(new File(outputDir));
    }

    public JarBuilder outputDir(File outputDir) throws IOException {
        this.outputDir = outputDir;
        return this;
    }

    public JarBuilder mainClass(String mainClass) {
        this.mainClass = mainClass;
        return this;
    }

    public JarBuilder mainClassToCompile(String mainClassToCompile) {
        this.mainClassToCompile = mainClassToCompile;
        return this;
    }

    private String[] getClasspathJars() {
        File[] jars = dependencyJarDir.listFiles((dir, name) -> name.endsWith("jar"));
        return Arrays.stream(Optional.ofNullable(jars).orElse(new File[]{}))
                .map(file -> {
                    try {
                        return file.getCanonicalPath();
                    } catch (Exception e) {
                        return "";
                    }
                })
                .filter(StringUtils::isNotEmpty)
                .toArray(String[]::new);
    }

    private File writeToJavaFile(String sourceCode) throws IOException {
        String mainClassName = mainClass.substring(mainClass.lastIndexOf('.') + 1) + ".java";
        String javaPath = workspace.getCanonicalPath() + File.separator + mainClassName;
        File javaCodeFile = new File(javaPath);
        FileUtils.write(javaCodeFile, sourceCode);
        return javaCodeFile;
    }

    private void purgeWorkspace() {
        if (workspace != null && workspace.exists() && purgeAfterBuild) {
            FileUtils.deleteQuietly(workspace);
        }
    }

    private void cleanWorkspace() throws IOException {
        if (workspace != null && workspace.exists()) {
            FileUtils.cleanDirectory(workspace);
        }
    }

    public String getMainClassFromJar(File jarFile) throws MalformedURLException {
        URL url = jarFile.toURI().toURL();
        String manifest = FileUtil.readJarManifest(url, "Main-Class");
        return manifest;
    }

    private void checkArgs() throws Exception {
        checkDir(workspace);
        if (sourceJar == null || !sourceJar.exists()) {
            throw new IllegalArgumentException("Invalid source Jar argument: null or not exist");
        }
        if (StringUtils.isEmpty(mainClass)) {
            mainClass = getMainClassFromJar(sourceJar);
            if (StringUtils.isEmpty(mainClass)) {
                throw new IllegalArgumentException("Invalid main class argument: null");
            }
        }
        if (outputDir == null) {
            outputDir = new File(workspace + File.separator + "target");
        }
        checkDir(outputDir);

        if (buildClassDir == null) {
            buildClassDir = workspace;
        }
        checkDir(buildClassDir);
        checkDir(dependencyJarDir);
    }

    private void checkDir(File dir) {
        checkDir(dir, true);
    }

    private void checkDir(File dir, boolean isMkdirs) {
        if (dir == null) {
            throw new IllegalArgumentException("Invalid dir: " + dir.getPath());
        }
        if (!dir.exists() && isMkdirs) {
            dir.mkdirs();
        }
        if (!dir.exists()) {
            throw new IllegalArgumentException("Dir not found: " + dir.getPath());
        }
        if (!dir.isDirectory()) {
            throw new IllegalArgumentException("Not regular directory: " + dir.getPath());
        }
    }

    public File compile() throws Exception {
        try {
            // 1. Check arguments
            checkArgs();
            cleanWorkspace();

            // 2. Get classpath Jar(s)
            String[] classpathJars = getClasspathJars();
            String classpath = String.join(";", classpathJars);

            // 3. Write modified class to file system
            File sourceCodeFile = writeToJavaFile(mainClassToCompile);

            // 4. Run javac
            String cmd = String.format("javac -encoding UTF-8 -cp \"%s\" -d \"%s\" \"%s\"", classpath, buildClassDir.getCanonicalPath(), sourceCodeFile.getCanonicalPath());
            LocalShellExecutor.ExecutorResult result = LocalShellExecutor.executeReturnString(cmd, "GBK");
            if (result.hasError()) {
                throw new RuntimeException("Error on execute javac: " + result.getError());
            }

            // 5. Rebuild Jar
            FileUtils.copyFileToDirectory(sourceJar, workspace, true);
            File jarToRebuild = new File(workspace + File.separator + sourceJar.getName());
            if (!jarToRebuild.exists()) {
                throw new RuntimeException("Error on copy Jar file to workspace");
            }
            cmd = String.format("cd \"%s\" & jar uf \"%s\" \"%s\"", jarToRebuild.getParent(), jarToRebuild.getName(), "com");
            result = LocalShellExecutor.executeReturnString(cmd, "GBK");
            if (result.hasError()) {
                throw new RuntimeException("Error on execute jar -uf: " + result.getError());
            }
            FileUtils.moveFileToDirectory(jarToRebuild, outputDir, true);
            File jarFileOutput = new File(outputDir + File.separator + jarToRebuild.getName());
            if (!jarFileOutput.exists()) {
                throw new RuntimeException("Error on copy Jar file to out dir");
            }

            return jarFileOutput;
        } finally {
            // 6. Purge workspace
            purgeWorkspace();
        }
    }
}

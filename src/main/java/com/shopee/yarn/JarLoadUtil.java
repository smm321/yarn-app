package com.shopee.yarn;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JarLoadUtil {
    private static final Logger LOG = Logger.getLogger(JarLoadUtil.class.getName());

    public static Map load(Configuration conf) {
        String path = "/dw/datax/";
        List<String> list = null;
        try {
            list = getAllFilePath(new Path(path), FileSystem.get(conf));
        } catch (IOException e) {
            e.printStackTrace();
        }
        Map resources = new HashMap();
        list.forEach(o -> {
            LocalResource jar = Records.newRecord(LocalResource.class);
            String myJar = o.substring(o.lastIndexOf("/") + 1);
            if (myJar.endsWith(".jar") || myJar.contains("db.config")) {
                String jarFile = path + myJar;
                LOG.info(jarFile);
                Path jarPath = new Path(jarFile);
                FileStatus jarFileStatus = null;
                try {
                    jarFileStatus = FileSystem.get(conf).getFileStatus(jarPath);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                jar.setResource(ConverterUtils.getYarnUrlFromPath(jarFileStatus.getPath()));
                jar.setSize(jarFileStatus.getLen());
                jar.setTimestamp(jarFileStatus.getModificationTime());
                jar.setType(LocalResourceType.FILE);
                jar.setVisibility(LocalResourceVisibility.PUBLIC);
                resources.put(myJar, jar);
            }
        });

        return resources;
    }

    public static List<String> getAllFilePath(Path filePath, FileSystem fs) throws IOException {
        List<String> fileList = new ArrayList<String>();
        FileStatus[] fileStatus = fs.listStatus(filePath);
        for (FileStatus fileStat : fileStatus) {
            if (fileStat.isDirectory()) {
                continue;
            } else {
                fileList.add(fileStat.getPath().toString());
            }
        }
        return fileList;
    }

    public static void setupAppMasterEnv(Configuration conf, Map<String, String> appMasterEnv) {
        StringBuilder classPathEnv = new StringBuilder();
        classPathEnv.append(ApplicationConstants.Environment.CLASSPATH.$()).append(File.pathSeparatorChar);
        classPathEnv.append("./*");

        for (String c : conf.getStrings(
                YarnConfiguration.YARN_APPLICATION_CLASSPATH,
                YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH)) {
            classPathEnv.append(File.pathSeparatorChar);
            classPathEnv.append(c.trim());
        }
        classPathEnv.append(ApplicationConstants.CLASS_PATH_SEPARATOR).append("./log4j.properties");
        String envStr = classPathEnv.toString();
        LOG.info("env: " + envStr);
        appMasterEnv.put(ApplicationConstants.Environment.CLASSPATH.name(), envStr);
    }
}

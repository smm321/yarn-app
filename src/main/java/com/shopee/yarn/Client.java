package com.shopee.yarn;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.Records;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

public class Client {
    private static final Logger LOG = Logger.getLogger(Client.class.getName());
    private YarnClient yarnClient;
    private Configuration conf;

    @Parameter(names = {"-" + Constants.OPT_APPNAME, "--" + Constants.OPT_APPNAME})
    private String appname;
    @Parameter(names = {"-" + Constants.OPT_CONTAINER_MEM, "--" + Constants.OPT_CONTAINER_MEM})
    private int containerMem;
    @Parameter(names = {"-" + Constants.OPT_POSITION_DATE, "--" + Constants.OPT_POSITION_DATE})
    private String positionDate;
    @Parameter(names = {"-" + Constants.OPT_ENV, "--" + Constants.OPT_ENV})
    private String env;

    @Parameter(names = {"-" + Constants.OPT_COMMAND, "--" + Constants.OPT_COMMAND})
    private String command;
    private int applicationMasterMem = 1024;

    private int containerCount = 1;
    private String applicationMasterClassName;

    public Client() throws Exception {
        this.conf = new YarnConfiguration();
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        conf.set("ipc.client.connection.maxidletime", "20000");
        conf.set("ipc.client.rpc-timeout.ms", "20000");
        this.yarnClient = YarnClient.createYarnClient();
        yarnClient.init(conf);
    }

    public boolean run() throws IOException, YarnException, InterruptedException {
        yarnClient.start();
        YarnClientApplication app = yarnClient.createApplication();
        ApplicationSubmissionContext appContext = app.getApplicationSubmissionContext();
        ApplicationId appId = appContext.getApplicationId();
        appContext.setApplicationName(this.appname);

        // Set up the container launch context for AM.
        ContainerLaunchContext amContainer = Records.newRecord(ContainerLaunchContext.class);
        Map resources = JarLoadUtil.load(conf);
        amContainer.setLocalResources(resources);
        Map<String, String> appMasterEnv = new HashMap<String, String>();
        setupAppMasterEnv(appMasterEnv);
        amContainer.setEnvironment(appMasterEnv);

        Resource capability = Records.newRecord(Resource.class);
        capability.setMemory(this.applicationMasterMem);
        capability.setVirtualCores(1);
        amContainer.setCommands(Collections.singletonList(this.getCommand()));

        appContext.setAMContainerSpec(amContainer);
        appContext.setResource(capability);
        appContext.setQueue("root.warehouse");
        yarnClient.submitApplication(appContext);

        return this.monitorApplication(appId);
    }

    private String getCommand() {
        StringBuilder sb = new StringBuilder();
        sb.append(Environment.JAVA_HOME.$()).append("/bin/java").append(" ");
        sb.append("-Xmx").append(this.applicationMasterMem).append("M").append(" ");
        sb.append("com.shopee.yarn.SampleAM").append(" ");
        sb.append("--").append(Constants.OPT_CONTAINER_MEM).append(" ").append(this.containerMem+200).append(" ");
        sb.append("--").append(Constants.OPT_CONTAINER_COUNT).append(" ").append(this.containerCount).append(" ");

        StringBuilder datax = new StringBuilder();
        datax.append("$JAVA_HOME/bin/java ").append(" -Xmx").append(containerMem).append("M")
                .append(" -DpositionDate=").append(positionDate)
                .append(" com.alibaba.datax.core.Engine ")
                .append(" -jobid 1")
                .append(" -job ").append(appname)
                .append(" -mode standalone")
                .append(" -env ").append(env);
        sb.append("--").append(Constants.OPT_COMMAND).append(" '").append(StringEscapeUtils.escapeJava(datax.toString())).append("' ");

        sb.append("1> ").append(ApplicationConstants.LOG_DIR_EXPANSION_VAR).append("/stdout").append(" ");
        sb.append("2> ").append(ApplicationConstants.LOG_DIR_EXPANSION_VAR).append("/stderr");
        String r = sb.toString();
        LOG.info("submit datax command : " + r);
        return r;
    }

    private boolean monitorApplication(ApplicationId appId) throws YarnException, IOException, InterruptedException {
        ApplicationReport appReport = yarnClient.getApplicationReport(appId);
        YarnApplicationState appState = appReport.getYarnApplicationState();
        while (appState != YarnApplicationState.FINISHED
                && appState != YarnApplicationState.KILLED
                && appState != YarnApplicationState.FAILED) {
            Thread.sleep(2000);
            appReport = yarnClient.getApplicationReport(appId);
            appState = appReport.getYarnApplicationState();
            LOG.info(String.format("Application report for %s (state: %s)", appId,appState));
        }
        FinalApplicationStatus status = appReport.getFinalApplicationStatus();
        appState = appReport.getYarnApplicationState();
        if (appState == YarnApplicationState.FINISHED) {
            LOG.info(String.format("diagnostics: %s", appReport.getDiagnostics()));
            LOG.info(String.format("ApplicationMaster host: %s", appReport.getHost()));
            LOG.info(String.format("ApplicationMaster RPC port: %s", appReport.getRpcPort()));
            LOG.info(String.format("queue: %s", appReport.getQueue()));
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            LOG.info(String.format("start time: %s", sdf.format(new Date(appReport.getStartTime()))));
            if (status == FinalApplicationStatus.SUCCEEDED) {
                LOG.info(String.format("final status: %s", "SUCCEEDED"));
            } else {
                LOG.info(String.format("final status: %s", "failed"));
            }
            LOG.info(String.format("tracking URL: %s", appReport.getTrackingUrl()));
            LOG.info(String.format("user: %s", appReport.getUser()));
            if(status == FinalApplicationStatus.SUCCEEDED){
                return true;
            }
        }
        return false;
    }

    private void setupAppMasterEnv(Map<String, String> appMasterEnv) {
        StringBuilder classPathEnv = new StringBuilder();
        classPathEnv.append(Environment.CLASSPATH.$()).append(File.pathSeparatorChar);
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
        appMasterEnv.put(Environment.CLASSPATH.name(), envStr);
    }

    public static void main(String[] args) {
        try {
            Client c = new Client();
            boolean r = false;
            new JCommander(c, args);
            r = c.run();

            if (r) {
                System.exit(0);
            }
            System.exit(-1);
        }catch (Exception e){
            e.printStackTrace();
            System.exit(-1);
        }

    }
}



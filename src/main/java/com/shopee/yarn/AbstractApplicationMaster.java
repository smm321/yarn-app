package com.shopee.yarn;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.NMClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.Records;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.beust.jcommander.Parameter;

public abstract class AbstractApplicationMaster {
    private AMRMClientAsync<ContainerRequest> resourceManager;
    private NMClient nodeManager;

    private Configuration conf;
    protected static final Logger LOG = Logger.getLogger(AbstractApplicationMaster.class.getName());

    @Parameter(names = {"-" + Constants.OPT_CONTAINER_MEM, "--" + Constants.OPT_CONTAINER_MEM})
    private int containerMem;

    @Parameter(names = {"-" + Constants.OPT_CONTAINER_COUNT, "--" + Constants.OPT_CONTAINER_COUNT})
    private int containerCount;

    @Parameter(names = {"-" + Constants.OPT_COMMAND, "--" + Constants.OPT_COMMAND})
    private String command;

    private int appMasterRpcPort = 0;
    private String appMasterTrackingUrl = "";
    private boolean done;
    private boolean isSuccess = false;
    private String errMsg = "";

    public AbstractApplicationMaster() {
        conf = new YarnConfiguration();
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
    }

    public void init(String[] args) {
        LOG.setLevel(Level.INFO);
        done = false;
    }

    public void shutdown(){
        done = true;
    }

    public boolean run() throws IOException, YarnException {
        LOG.info("ApplicationMaster::run");
        LOG.info("command: " + this.command);
        AMRMClientAsync.CallbackHandler rmListener = new RMCallbackHandler();
        resourceManager = AMRMClientAsync.createAMRMClientAsync(1000, rmListener);
        resourceManager.init(conf);
        resourceManager.start();
        nodeManager = NMClient.createNMClient();
        nodeManager.init(conf);
        nodeManager.start();
        resourceManager.registerApplicationMaster(NetUtils.getHostname(), appMasterRpcPort, appMasterTrackingUrl);

        ContainerRequest containerReq = setupContainerReqForRM();
        resourceManager.addContainerRequest(containerReq);
        LOG.info("add container success ");
        while (!done) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException ex) {
            }
        }
        if(isSuccess){
            resourceManager.unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED, "Application complete!", "");
        }else{
            resourceManager.unregisterApplicationMaster(FinalApplicationStatus.FAILED, errMsg, "");
        }

        return isSuccess;
    }

    private ContainerRequest setupContainerReqForRM() {
        Priority priority = Records.newRecord(Priority.class);
        priority.setPriority(0);
        Resource capability = Records.newRecord(Resource.class);
        ContainerRequest containerReq = new ContainerRequest(
                capability,
                null /* hosts String[] */,
                null /* racks String [] */,
                priority);
        return containerReq;
    }

    private class RMCallbackHandler implements AMRMClientAsync.CallbackHandler {
        public void onContainersCompleted(List<ContainerStatus> statuses) {
            LOG.info("onContainersCompleted ... ");
            for (ContainerStatus status : statuses) {
                int exitStatus = status.getExitStatus();
                LOG.info("####################################################");
                if(0 == exitStatus){
                    isSuccess = true;
                    LOG.info("datax execute success");
                }else{
                    LOG.info("datax execute failed");
                    LOG.info(status.getDiagnostics());
                    errMsg = status.getDiagnostics();
                }
                LOG.info("####################################################");
                done = true;
            }
        }

        public void onContainersAllocated(List<Container> containers) {
            LOG.info("onContainersAllocated ... ");
            int containerCnt = containers.size();

            for (int i = 0; i < containerCnt; i++) {
                Container c = containers.get(i);
                LOG.info(command);
                StringBuilder sb = new StringBuilder();
                ContainerLaunchContext ctx = Records.newRecord(ContainerLaunchContext.class);
                ctx.setLocalResources(JarLoadUtil.load(conf));
                Map<String, String> appMasterEnv = new HashMap<String, String>();
                JarLoadUtil.setupAppMasterEnv(conf, appMasterEnv);
                ctx.setEnvironment(appMasterEnv);
                ctx.setCommands(Collections.singletonList(
                        sb.append(command)
                                .append(" 1> ").append(ApplicationConstants.LOG_DIR_EXPANSION_VAR).append("/stdout")
                                .append(" 2> ").append(ApplicationConstants.LOG_DIR_EXPANSION_VAR).append("/stderr")
                                .toString()));
                try {
                    nodeManager.startContainer(c, ctx);
                } catch (YarnException e) {
                    e.printStackTrace();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }


        public void onNodesUpdated(List<NodeReport> updated) {
        }

        public void onError(Throwable e) {
            done = true;
            resourceManager.stop();
        }

        // Called when the ResourceManager wants the ApplicationMaster to shutdown
        // for being out of sync etc. The ApplicationMaster should not unregister
        // with the RM unless the ApplicationMaster wants to be the last attempt.
        public void onShutdownRequest() {
            done = true;
            resourceManager.stop();
        }

        public float getProgress() {
            return 0;
        }
    }

}

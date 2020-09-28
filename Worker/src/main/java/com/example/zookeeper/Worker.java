package com.example.zookeeper;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.slf4j.*;

import java.io.IOException;
import java.util.*;

public class Worker implements Watcher {
    private static final Logger LOG = LoggerFactory.getLogger(Worker.class);
    ZooKeeper zk;
    String hostPort;
    Random random = new Random();
    String serverId = Integer.toHexString(random.nextInt());
    String status = "Idle";
    AsyncCallback.StringCallback createWorkerCallback = new AsyncCallback.StringCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, String name) {
            switch (KeeperException.Code.get(rc)){
                case CONNECTIONLOSS:
                    register();
                    break;
                case OK:
                    LOG.info("Registered successfully: "+serverId);
                    break;
                case NODEEXISTS:
                    LOG.warn("Already registered: "+serverId);
                    break;
                default:
                    LOG.error("Something went wrong: "+ KeeperException.create(KeeperException.Code.get(rc), path));
            }
        }
    };

    AsyncCallback.StatCallback updateStatCallback = new AsyncCallback.StatCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, Stat stat) {
            switch (KeeperException.Code.get(rc)){
                case CONNECTIONLOSS:
                    updateStatus((String)ctx);
            }
        }
    };

    public Worker(String hostPort){
        this.hostPort = hostPort;
    }

    public void startZk() throws IOException {
        zk = new ZooKeeper(hostPort,15000,this);
    }

    public void stopZk() throws InterruptedException {
        zk.close();
    }

    public void register(){
        zk.create(
                "/workers/worker-"+serverId,
                status.getBytes(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.EPHEMERAL,
                createWorkerCallback,
                null
        );
    }

    synchronized void updateStatus(String status){
        if(status == this.status){
            zk.setData("/workers/worker-"+serverId, status.getBytes(), -1, updateStatCallback, status);
        }
    }

    public void setStatus(String status){
        this.status = status;
        updateStatus(status);
    }

    @Override
    public void process(WatchedEvent watchedEvent) {
        LOG.info(watchedEvent.toString()+", "+hostPort);
    }
}

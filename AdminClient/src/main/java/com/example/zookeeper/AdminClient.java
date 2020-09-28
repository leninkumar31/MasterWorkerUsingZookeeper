package com.example.zookeeper;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.*;

import java.io.IOException;
import java.util.Date;

public class AdminClient implements Watcher {
    private final static Logger LOG = LoggerFactory.getLogger(AdminClient.class);
    ZooKeeper zk;
    String hostPort;

    public AdminClient(String hostPort){
        this.hostPort = hostPort;
    }

    public void startZk() throws IOException {
        zk = new ZooKeeper(hostPort, 15000, this);
    }

    public void stopZk() throws InterruptedException {
        zk.close();
    }

    public void listState() throws KeeperException, InterruptedException {
        try{
            Stat stat = new Stat();
            byte[] masterData = zk.getData("/master", false, stat);
            Date startDate = new Date(stat.getCtime());
            LOG.info("Master: "+new String(masterData)+" since "+startDate);
        }catch (KeeperException.NodeExistsException e){
            LOG.warn("No master exists in the system");
        }
        LOG.info("Workers: ");
        for(String w: zk.getChildren("/workers", false)){
            byte[] workerData = zk.getData("/workers/"+w, false, null);
            LOG.info("Worker: "+ new String(workerData));
        }
        LOG.info("Tasks: ");
        for(String t: zk.getChildren("/assign", false)){
            LOG.info("Task: "+ t);
        }
    }

    @Override
    public void process(WatchedEvent watchedEvent) {
        LOG.info(watchedEvent + " , "+hostPort);
    }
}

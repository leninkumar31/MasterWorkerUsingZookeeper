package com.example.zookeeper;

import org.apache.zookeeper.*;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.logging.Logger;

public class Client implements Watcher {
    private static final Logger LOG = (Logger) LoggerFactory.getLogger(Client.class);
    ZooKeeper zk;
    String hostPort;

    public Client(String hostPort){
        this.hostPort = hostPort;
    }

    public void startZk() throws IOException {
        zk = new ZooKeeper(hostPort, 15000, this);
    }

    public void stopZk() throws InterruptedException {
        zk.close();
    }

    public String queueCommand(String cmd) throws Exception {
        while(true){
            try {
                String name = zk.create("/tasks/task-",
                        cmd.getBytes(),
                        ZooDefs.Ids.OPEN_ACL_UNSAFE,
                        CreateMode.PERSISTENT_SEQUENTIAL
                );
                return name;
            }catch (KeeperException.NodeExistsException e){
                throw new Exception("Already appears to be running: " + e);
            }catch (KeeperException.ConnectionLossException e){

            }
        }
    }

    @Override
    public void process(WatchedEvent watchedEvent) {
        LOG.info(watchedEvent.toString() +" ,"+hostPort);
    }
}

package com.example.zookeeper;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.slf4j.*;

import java.io.IOException;
import java.util.*;

public class Master implements Watcher {
    ZooKeeper zk;
    String hostPort;
    Random random  = new Random();
    String serverID = Integer.toHexString(random.nextInt());
    Boolean isLeader = Boolean.FALSE;
    private static final Logger LOG = (Logger) LoggerFactory.getLogger(Master.class);
    public Master(String hostPort){
        this.hostPort = hostPort;
    }

    public void startZk() throws IOException {
        zk = new ZooKeeper(hostPort, 15000, this);
    }

    public void stopZk() throws InterruptedException {
        zk.close();
    }

    public Boolean checkMaster() throws KeeperException, InterruptedException {
        while(true){
            Stat stat = new Stat();
            try {
                byte[] data = zk.getData("/master", false, stat);
                isLeader = new String(data).equals(serverID);
                return true;
            }catch (KeeperException.NodeExistsException e){
                return false;
            }catch (KeeperException.ConnectionLossException e){
            }
        }
    }

    public void runForMaster() throws InterruptedException, KeeperException {
        while(true) {
            try {
                zk.create("/master",
                        serverID.getBytes(),
                        ZooDefs.Ids.OPEN_ACL_UNSAFE,
                        CreateMode.EPHEMERAL);
                isLeader = Boolean.TRUE;
                break;
            } catch (KeeperException.NodeExistsException e) {
                isLeader = Boolean.FALSE;
                break;
            } catch (KeeperException.ConnectionLossException e) {

            }
            if(checkMaster())
                break;
        }
    }

    public void bootstrap(){
        createParent("/workers", new byte[0]);
        createParent("/assign", new byte[0]);
        createParent("/tasks", new byte[0]);
        createParent("/status", new byte[0]);
    }

    void createParent(String path, byte[] data){
        zk.create(
                path,
                data,
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT,
                createParentCallBack,
                data
        );
    }

    AsyncCallback.StringCallback createParentCallBack = new AsyncCallback.StringCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, String name) {
            switch (KeeperException.Code.get(rc)){
                case CONNECTIONLOSS:
                    createParent(path, (byte[])ctx);
                    break;
                case OK:
                    LOG.info("Parent created");
                    break;
                case NODEEXISTS:
                    LOG.warn("Parent already registered "+path);
                    break;
                default:
                    LOG.error("Something went wrong: "+KeeperException.create(KeeperException.Code.get(rc), path));
            }
        }
    };

    public Boolean getLeader() {
        return isLeader;
    }

    @Override
    public void process(WatchedEvent e){
        System.out.println(e);
    }
}

package com.example.zookeeper;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.slf4j.*;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;

public class Client implements Watcher, Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(Client.class);
    ZooKeeper zk;
    String hostPort;

    boolean connected = false;
    public boolean isConnected(){
        return connected;
    }

    boolean expired = false;
    public Client(String hostPort){
        this.hostPort = hostPort;
    }

    public void startZk() throws IOException {
        zk = new ZooKeeper(hostPort, 15000, this);
    }

    public void stopZk() throws InterruptedException {
        zk.close();
    }

    public void submitTask(String task, TaskObject taskCtx){
        taskCtx.setTask(task);
        zk.create("/tasks/task-",
                task.getBytes(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT_SEQUENTIAL,
                createTaskCallback,
                taskCtx);
    }

    AsyncCallback.StringCallback createTaskCallback = new AsyncCallback.StringCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, String name) {
            switch (KeeperException.Code.get(rc)){
                case CONNECTIONLOSS:
                    submitTask(((TaskObject)ctx).getTask(), (TaskObject)ctx);
                    break;
                case OK:
                    LOG.info("Task successfully created: "+name);
                    ((TaskObject)ctx).setTaskName(name);
                    watchStatus("/status/"+name.replace("/tasks/",""), ctx);
                    break;
                default:
                    LOG.error("Task creation failed: "+KeeperException.create(KeeperException.Code.get(rc), path));
            }
        }
    };

    ConcurrentHashMap statusMap = new ConcurrentHashMap();

    Watcher taskStatusChangeWatcher = new Watcher() {
        @Override
        public void process(WatchedEvent watchedEvent) {
            if(watchedEvent.getType()== Event.EventType.NodeCreated){
                assert watchedEvent.getPath().contains("/status/task-");
                assert statusMap.containsKey(watchedEvent.getPath());
                zk.getData(watchedEvent.getPath(),
                        false,
                        dataCallback,
                        statusMap.get(watchedEvent.getPath())
                );
            }
        }
    };

    void watchStatus(String path, Object ctx){
        statusMap.put(path, ctx);
        zk.exists(path,
                taskStatusChangeWatcher,
                taskStatusExistsCallback,
                ctx);
    }

    AsyncCallback.StatCallback taskStatusExistsCallback = new AsyncCallback.StatCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, Stat stat) {
            switch (KeeperException.Code.get(rc)){
                case CONNECTIONLOSS:
                    watchStatus(path, ctx);
                    break;
                case OK:
                    if(stat != null) {
                        zk.getData(path,
                                false,
                                dataCallback,
                                ctx
                        );
                    }
                    break;
                case NONODE:
                    LOG.info("znode is not yet created: "+path);
                    break;
                default:
                    LOG.error("Error occurred while executing exists: "+KeeperException.create(KeeperException.Code.get(rc), path));
            }
        }
    };

    AsyncCallback.DataCallback dataCallback = new AsyncCallback.DataCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
            switch (KeeperException.Code.get(rc)){
                case CONNECTIONLOSS:
                    zk.getData(path,
                            false,
                            dataCallback,
                            ctx
                    );
                    break;
                case OK:
                    String taskResult = new String(data);
                    LOG.info("Task "+path +", "+taskResult);
                    assert ctx!=null;
                    ((TaskObject)ctx).setStatus(taskResult.contains("done"));
                    zk.delete(path, -1, deleteCallBack, null);
                    statusMap.remove(path);
                case NONODE:
                    LOG.warn("Path is already deleted");
                    break;
                default:
                    LOG.error("Error occurred while getting node data: "+KeeperException.create(KeeperException.Code.get(rc), path));
            }
        }
    };

    AsyncCallback.VoidCallback deleteCallBack = new AsyncCallback.VoidCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx) {
            switch (KeeperException.Code.get(rc)){
                case CONNECTIONLOSS:
                    zk.delete(path, -1, deleteCallBack, ctx);
                    break;
                case OK:
                    LOG.info("Node is deleted successfully: "+path);
                    break;
                case NONODE:
                    LOG.warn("Node doesn't exists: "+path);
                    break;
                default:
                    LOG.error("Error occurred while deleting node: "+KeeperException.create(KeeperException.Code.get(rc), path));
            }
        }
    };


    @Override
    public void process(WatchedEvent watchedEvent) {
        LOG.info(watchedEvent.toString() +" ,"+hostPort);
        if(watchedEvent.getType()== Event.EventType.None) {
            switch (watchedEvent.getState()) {
                case SyncConnected:
                    connected = true;
                    break;
                case Disconnected:
                    connected = false;
                    break;
                case Expired:
                    connected = false;
                    expired = true;
                    LOG.info("Exiting due to session expiration");
                    break;
                default:
                    LOG.error("Unknown state: "+watchedEvent.getState());
                    break;
            }
        }
    }

    @Override
    public void close() throws IOException {
        LOG.info("Closing");
        try {
            zk.close();
        } catch (InterruptedException e) {
            LOG.warn("Zookeeper interrupted while closing");
        }
    }
}

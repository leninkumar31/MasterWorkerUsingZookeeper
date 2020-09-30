package com.example.zookeeper;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.slf4j.*;

import java.io.Closeable;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class Worker implements Watcher, Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(Worker.class);
    ZooKeeper zk;
    String hostPort;
    Random random = new Random();
    String serverId = Integer.toHexString(random.nextInt());
    String status = "Idle";
    private ThreadPoolExecutor executor;

    public Worker(String hostPort){
        this.hostPort = hostPort;
        executor = new ThreadPoolExecutor(1, 1, 1000L, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<Runnable>(200));
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

    public void bootstrap(){
        createAssignNode();
    }

    void createAssignNode(){
        zk.create(
                "/tasks/worker-"+serverId,
                serverId.getBytes(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.EPHEMERAL,
                createAssignCallback,
                null
        );
    }

    AsyncCallback.StringCallback createAssignCallback = new AsyncCallback.StringCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, String name) {
            switch (KeeperException.Code.get(rc)){
                case CONNECTIONLOSS:
                    createAssignNode();
                    break;
                case NODEEXISTS:
                    LOG.warn("assign/worker-"+serverId+" already created");
                    break;
                case OK:
                    LOG.info("assign/worker-"+serverId + "is successfully created");
                    break;
                default:
                    LOG.error("Some thing wrong happened while creating node: "+KeeperException.create(KeeperException.Code.get(rc), path));
            }
        }
    };

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

    public void setStatus(String status){
        this.status = status;
        updateStatus(status);
    }

    synchronized void updateStatus(String status){
        if(status == this.status){
            zk.setData("/workers/worker-"+serverId, status.getBytes(), -1, updateStatCallback, status);
        }
    }

    AsyncCallback.StatCallback updateStatCallback = new AsyncCallback.StatCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, Stat stat) {
            switch (KeeperException.Code.get(rc)){
                case CONNECTIONLOSS:
                    updateStatus((String)ctx);
            }
        }
    };

    Watcher taskChangeWatcher = new Watcher() {
        @Override
        public void process(WatchedEvent watchedEvent) {
            if(watchedEvent.getType()== Event.EventType.NodeChildrenChanged){
                assert new String("/assign/worker-"+serverId).equals(watchedEvent.getPath());
                getTasks();
            }
        }
    };

    void getTasks(){
        zk.getChildren("/assign/worker-"+serverId,
                taskChangeWatcher,
                getTasksCallback,
                null);
    }
    ChildrenCache assignedTasksCache = new ChildrenCache();
    AsyncCallback.ChildrenCallback getTasksCallback = new AsyncCallback.ChildrenCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, List<String> tasks) {
            switch (KeeperException.Code.get(rc)){
                case CONNECTIONLOSS:
                    getTasks();
                    break;
                case OK:
                    if(tasks != null){
                        executor.execute(new Runnable() {
                            List<String> children;
                            DataCallback cb;
                            public Runnable init(List<String> children, DataCallback cb){
                                this.children = children;
                                this.cb = cb;
                                return this;
                            }
                            @Override
                            public void run() {
                                if(children==null) {
                                    return;
                                }
                                LOG.info("Looping into tasks");
                                setStatus("Working");
                                for(String task: children){
                                    LOG.trace("New Task: {}", task);
                                    zk.getData("/assign/worker-"+serverId+"/"+task,
                                            false,
                                            cb,
                                            task);
                                }
                            }
                        }.init(assignedTasksCache.addAndSet(tasks), taskDataCallback));
                    }
                    break;
                default:
                    LOG.error("Something wrong happened: "+KeeperException.create(KeeperException.Code.get(rc),path));
            }
        }
    };

    AsyncCallback.DataCallback taskDataCallback = new AsyncCallback.DataCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
            switch (KeeperException.Code.get(rc)){
                case CONNECTIONLOSS:
                    zk.getData(path, false, taskDataCallback, ctx);
                    break;
                case OK:
                    executor.execute(new Runnable() {
                        byte[] data;
                        Object ctx;
                        public Runnable init(byte[] data, Object ctx){
                            this.data = data;
                            this.ctx = ctx;
                            return this;
                        }
                        @Override
                        public void run() {
                            LOG.info("Executing the task: "+new String(data));
                            zk.create("/status/"+ (String) ctx,
                                    "done".getBytes(),
                                    ZooDefs.Ids.OPEN_ACL_UNSAFE,
                                    CreateMode.PERSISTENT,
                                    taskStatusCreateCallback,
                                    null);
                            zk.delete("assign/worker-"+serverId+"/"+ (String)ctx, -1, taskDeleteCallback, null);
                        }
                    }.init(data, ctx));
                    break;
                default:
                    LOG.error("getData failed: "+KeeperException.create(KeeperException.Code.get(rc),path));
            }
        }
    };

    AsyncCallback.StringCallback taskStatusCreateCallback = new AsyncCallback.StringCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, String name) {
            switch (KeeperException.Code.get(rc)){
                case CONNECTIONLOSS:
                    zk.create(path,
                            "done".getBytes(),
                            ZooDefs.Ids.OPEN_ACL_UNSAFE,
                            CreateMode.PERSISTENT,
                            taskStatusCreateCallback,
                            null);
                    break;
                case OK:
                    LOG.info("Status znode is created successfully");
                    break;
                case NODEEXISTS:
                    LOG.warn("Node exists: "+path);
                    break;
                default:
                    LOG.error("create failed: "+KeeperException.create(KeeperException.Code.get(rc),path));
            }
        }
    };

    AsyncCallback.VoidCallback taskDeleteCallback = new AsyncCallback.VoidCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx) {
            switch (KeeperException.Code.get(rc)){
                case CONNECTIONLOSS:
                    zk.delete(path, -1, taskDeleteCallback, null);
                    break;
                case OK:
                    LOG.info("znode deleted successfully");
                    break;
                case NONODE:
                    LOG.warn("znode doesn't exists: "+path);
                    break;
                default:
                    LOG.error("Delete znode failed: "+KeeperException.create(KeeperException.Code.get(rc),path));
            }
        }
    };

    @Override
    public void process(WatchedEvent watchedEvent) {
        LOG.info(watchedEvent.toString()+", "+hostPort);
    }

    @Override
    public void close() throws IOException{
        LOG.info("Closing the zk session");
        try {
            zk.close();
        } catch (InterruptedException e) {
            LOG.warn("zookeeper interrupted while closing");
        }
    }
}

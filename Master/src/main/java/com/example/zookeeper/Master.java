package com.example.zookeeper;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.slf4j.*;

import java.io.Closeable;
import java.io.IOException;
import java.util.*;

public class Master implements Watcher, Closeable {
    enum masterstates {RUNNING, ELECTED, NOTELECTED};
    private masterstates state = masterstates.RUNNING;
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

    @Override
    public void close() throws IOException{
        if(zk != null) {
            try {
                zk.close();
            } catch (InterruptedException e) {
                LOG.warn("Interrupted while closing zookeeper connection");
            }
        }
    }

    void checkMaster() {
        zk.getData("/master",
                false,
                checkMasterCallback,
                null);
    }

    AsyncCallback.DataCallback checkMasterCallback = new AsyncCallback.DataCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
            switch (KeeperException.Code.get(rc)){
                case CONNECTIONLOSS:
                    checkMaster();
                    break;
                case NONODE:
                    runForMaster();
                    break;
                case OK:
                    if(serverID.equals(new String(data))){
                        state = masterstates.ELECTED;
                        isLeader = Boolean.TRUE;
                        // TODO: takeLeadership
                    }else{
                        state = masterstates.NOTELECTED;
                        masterExists();
                    }
                default:
                    LOG.error("Error while reading the data: "+KeeperException.create(KeeperException.Code.get(rc),path));
            }
        }
    };

    void masterExists(){
        zk.exists("/master",
                masterExistsWatcher,
                masterExistsCallback,
                null
        );
    }

    Watcher masterExistsWatcher = new Watcher(){
        @Override
        public void process(WatchedEvent e){
            if(e.getType() == Event.EventType.NodeDeleted){
                assert "/master".equals(e.getPath());
                runForMaster();
            }
        }
    };

    AsyncCallback.StatCallback masterExistsCallback = new AsyncCallback.StatCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, Stat stat) {
            switch (KeeperException.Code.get(rc)){
                case CONNECTIONLOSS:
                    masterExists();
                    break;
                case OK:
                    if(stat == null){
                        state = masterstates.RUNNING;
                        runForMaster();
                    }
                    break;
                default:
                    checkMaster();
                    break;
            }
        }
    };


    public void runForMaster() {
        zk.create("/master",
                serverID.getBytes(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.EPHEMERAL,
                createMasterCallback,
                null);
    }

    AsyncCallback.StringCallback createMasterCallback = new AsyncCallback.StringCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, String name) {
            switch (KeeperException.Code.get(rc)){
                case CONNECTIONLOSS:
                    checkMaster();
                    break;
                case NODEEXISTS:
                    state = masterstates.NOTELECTED;
                    masterExists();
                    break;
                case OK:
                    state = masterstates.ELECTED;
                    // TODO: takeLeadership
                    break;
                default:
                    state = masterstates.ELECTED;
                    LOG.error("Something terrible happened: "+KeeperException.create(KeeperException.Code.get(rc),path));
            }
        }
    };

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

    Watcher workerChangeWatcher = new Watcher() {
        @Override
        public void process(WatchedEvent watchedEvent) {
            if(watchedEvent.getType() == Event.EventType.NodeChildrenChanged){
                assert "/workers".equals(watchedEvent.getPath());
                getWorkers();
            }
        }
    };

    void getWorkers(){
        zk.getChildren("/workers",
                workerChangeWatcher,
                workerGetChildrenCallback,
                null);
    }

    AsyncCallback.ChildrenCallback workerGetChildrenCallback = new AsyncCallback.ChildrenCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, List<String> children) {
            switch (KeeperException.Code.get(rc)){
                case CONNECTIONLOSS:
                    getWorkers();
                    break;
                case OK:
                    LOG.info("Succefully got the list of children: "+children.size()+ " workers");
                    reassignAndSet(children);
                    break;
                default:
                    LOG.error("getChildren failed: "+ KeeperException.create(KeeperException.Code.get(rc),path));
            }
        }
    };

    ChildrenCache workersCache;
    void reassignAndSet(List<String> children){
        List<String> toProcess;
        if(workersCache == null){
            workersCache = new ChildrenCache(children);
            toProcess = null;
        }else{
            LOG.info("Removing and Setting");
            toProcess = workersCache.removeAndSet(children);
        }
        if(toProcess != null){
            for(String s : toProcess){
                // TODO: getAbsentWorkersTasks
            }
        }
    }

    Watcher tasksChangeWatcher = new Watcher() {
        @Override
        public void process(WatchedEvent watchedEvent) {
            if (watchedEvent.getType()== Event.EventType.NodeChildrenChanged){
                assert "/tasks".equals(watchedEvent.getPath());
                getTasks();
            }
        }
    };

    ChildrenCache tasksCache;
    void getTasks(){
        zk.getChildren("/tasks",
                tasksChangeWatcher,
                getTasksCallback,
                null
        );
    }

    AsyncCallback.ChildrenCallback getTasksCallback = new AsyncCallback.ChildrenCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, List<String> tasks) {
            switch (KeeperException.Code.get(rc)){
                case CONNECTIONLOSS:
                    getTasks();
                    break;
                case OK:
                    List<String> toProcess;
                    if(tasksCache == null){
                        tasksCache = new ChildrenCache(tasks);
                        toProcess = tasks;
                    }else{
                        toProcess = tasksCache.addAndSet(tasks);
                    }
                    if(toProcess != null){
                        assignTasks(toProcess);
                    }
                default:
                    LOG.error("getChildren failed: " + KeeperException.create(KeeperException.Code.get(rc), path));
            }
        }
    };

    void assignTasks(List<String> tasks){
        for(String task: tasks){
            getTaskData(task);
        }
    }

    void getTaskData(String task){
        zk.getData("/tasks/"+task,
                false,
                getDataCallback,
                task);
    }

    AsyncCallback.DataCallback getDataCallback = new AsyncCallback.DataCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
            switch (KeeperException.Code.get(rc)){
                case CONNECTIONLOSS:
                    getTaskData((String)ctx);
                    break;
                case OK:
                    List<String> workers = workersCache.getList();
                    String choosenWorker = workers.get(random.nextInt(workers.size()));
                    String assignmentPath = "/workers/"+choosenWorker+"/"+(String)ctx;
                    LOG.info("Assigned path: "+ assignmentPath);
                    createAssignmentPath(assignmentPath, data);
                    break;
                default:
                    LOG.error("getData failed: "+ KeeperException.create(KeeperException.Code.get(rc), path));
            }
        }
    };

    void createAssignmentPath(String path, byte[] data){
        zk.create(path,
                data,
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT,
                assignTaskCallback,
                data);
    }

    AsyncCallback.StringCallback assignTaskCallback = new AsyncCallback.StringCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, String name) {
            switch (KeeperException.Code.get(rc)){
                case CONNECTIONLOSS:
                    createAssignmentPath(path, (byte[])ctx);
                    break;
                case OK:
                    LOG.info("Task assigned successfullt: "+ name);
                    deleteTask(name.substring(name.lastIndexOf("/")+1));
                    break;
                case NODEEXISTS:
                    LOG.warn("task Already assigned");
                    break;
                default:
                    LOG.error("Something wrong happened while assigning task: " + KeeperException.create(KeeperException.Code.get(rc), path));
            }
        }
    };

    void deleteTask(String task){
        zk.delete(task, -1, deleteTaskCallback, null);
    }

    AsyncCallback.VoidCallback deleteTaskCallback = new AsyncCallback.VoidCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx) {
            switch (KeeperException.Code.get(rc)){
                case CONNECTIONLOSS:
                    deleteTask((String)ctx);
                    break;
                case NONODE:
                    LOG.info("Task is already deleted");
                    break;
                case OK:
                    LOG.info("task is deleted successfully");
                    break;
                default:
                    LOG.error("There was something wrong with deleting task:" + KeeperException.create(KeeperException.Code.get(rc), path));
            }
        }
    };
}

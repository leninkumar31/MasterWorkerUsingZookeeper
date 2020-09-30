package com.example.zookeeper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;

public class TaskObject {
    private String task;
    private String taskName;
    private boolean done = false;
    private boolean successful = false;
    private CountDownLatch latch = new CountDownLatch(1);
    private static final Logger LOG = LoggerFactory.getLogger(TaskObject.class);

    public String getTask(){
        return task;
    }

    public void setTask(String task){
        this.task = task;
    }

    public String getTaskName(){
        return taskName;
    }

    public void setTaskName(String taskName){
        this.taskName = taskName;
    }

    public void setStatus(boolean status){
        this.successful = status;
        done = true;
        latch.countDown();
    }

    public void waitUntilDone(){
        try {
            latch.await();
        } catch (InterruptedException e) {
            LOG.error("Interrupted while executing for task to get done");
        }
    }

    public boolean isDone(){
        return done;
    }

    public boolean isSuccessful(){
        return successful;
    }
}

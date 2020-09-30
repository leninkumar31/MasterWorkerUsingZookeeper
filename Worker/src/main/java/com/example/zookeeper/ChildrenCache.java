package com.example.zookeeper;

import java.util.ArrayList;
import java.util.List;

public class ChildrenCache {
    List<String> childrenCache;

    ChildrenCache(){
        childrenCache = null;
    }

    ChildrenCache(List<String> childrenCache){
        this.childrenCache = childrenCache;
    }

    public List<String> getList(){
        return childrenCache;
    }

    public List<String> removeAndSet(List<String> newChildren){
        List<String> diff = null;
        if(childrenCache != null){
            for(String child: childrenCache){
                if(!newChildren.contains(child)){
                    if(diff==null){
                        diff = new ArrayList<>();
                    }
                    diff.add(child);
                }
            }
        }
        this.childrenCache = newChildren;
        return diff;
    }

    public List<String> addAndSet(List<String> newTasks){
        List<String> diff = null;
        if(childrenCache==null){
            diff = new ArrayList<>(newTasks);
        }else{
            for(String task: newTasks){
                if (!childrenCache.contains(task)){
                    if(diff == null){
                        diff = new ArrayList<>();
                    }
                    diff.add(task);
                }
            }
        }
        this.childrenCache = newTasks;
        return diff;
    }
}

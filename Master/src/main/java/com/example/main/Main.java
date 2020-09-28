package com.example.main;

import com.example.zookeeper.Master;
import org.apache.log4j.PropertyConfigurator;

import java.io.IOException;

/**
 * Hello world!
 *
 */
public class Main
{
    public static void main( String[] args ) throws Exception {
        String log4jConfPath = "/usr/local/etc/zookeeper/log4j.properties";
        PropertyConfigurator.configure(log4jConfPath);
        Master m = new Master(args[0]);
        m.startZk();
        m.runForMaster();
        if(m.getLeader()){
            System.out.println("I am the leader");
            m.bootstrap();
            Thread.sleep(60000);
        }else{
            System.out.println("Someone else is the the leader");
        }
        m.stopZk();
    }
}

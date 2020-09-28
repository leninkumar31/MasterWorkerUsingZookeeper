package com.example.main;

import com.example.zookeeper.Worker;
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
        Worker w = new Worker(args[0]);
        w.startZk();
        w.register();
        Thread.sleep(60000);
        w.startZk();
    }
}

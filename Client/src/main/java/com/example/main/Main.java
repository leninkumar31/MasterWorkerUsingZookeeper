package com.example.main;

import com.example.zookeeper.Client;
import org.apache.log4j.PropertyConfigurator;

/**
 * Hello world!
 *
 */
public class Main
{
    public static void main( String[] args ) throws Exception
    {
        String log4jConfPath = "/usr/local/etc/zookeeper/log4j.properties";
        PropertyConfigurator.configure(log4jConfPath);
        Client client = new Client(args[0]);
        client.startZk();
        String name = client.queueCommand(args[1]);
        System.out.println("Command queued: "+name);
        Thread.sleep(60000);
        client.stopZk();
    }
}

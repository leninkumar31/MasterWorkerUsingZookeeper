package com.example.main;

import com.example.zookeeper.Client;
import com.example.zookeeper.TaskObject;
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
        while(!client.isConnected()){
            Thread.sleep(100);
        }

        TaskObject task1 = new TaskObject();
        TaskObject task2 = new TaskObject();

        client.submitTask("First Task", task1);
        client.submitTask("Second Task", task2);

        task1.waitUntilDone();
        task2.waitUntilDone();
    }
}

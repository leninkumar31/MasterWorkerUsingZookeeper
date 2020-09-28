package com.example.main;

import org.apache.log4j.PropertyConfigurator;

/**
 * Hello world!
 *
 */
public class Main
{
    public static void main( String[] args )
    {
        String log4jConfPath = "/usr/local/etc/zookeeper/log4j.properties";
        PropertyConfigurator.configure(log4jConfPath);

    }
}

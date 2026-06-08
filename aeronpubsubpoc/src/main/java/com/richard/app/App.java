package com.richard.app;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Hello world!
 */
public class App {
	private static final Logger log = LoggerFactory.getLogger(App.class); 
    public static void main(String[] args) throws Exception{
        AeronPublisher pub = new AeronPublisher(3999, 1999);
        pub.start();
        AeronSubscriber sub = new AeronSubscriber(3999, 1999, System.out::println);
        sub.start();
        Thread.sleep(1000);
        long position = pub.publish("testing");
        log.info("position: {}", position);
        pub.stop();
        sub.stop();
    }
}

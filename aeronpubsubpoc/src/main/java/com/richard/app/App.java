package com.richard.app;

import java.util.ArrayList;
import java.util.List;

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
        List<String> messages = new ArrayList<>();
        AeronSubscriber sub = new AeronSubscriber(3999, 1999, s->{log.info(s); messages.add(s);});
        sub.start();
        Thread.sleep(1000);
        Thread pubt = new Thread(()-> {
	        for (int i = 0; i < 100_000; i++) {
	        	long position = pub.publish("testing" + i);
	        	log.info("{} position: {}", i, position);
	        
	        }
        }, "pubt");
        pubt.start();
        pubt.join();        
        
        pub.stop();
        Thread.sleep(1000);
        sub.stop();
        log.info("message received count: {}", messages.size());
    }
}

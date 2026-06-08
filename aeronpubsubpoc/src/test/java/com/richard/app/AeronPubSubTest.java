package com.richard.app;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class AeronPubSubTest {
	private static final Logger log = LoggerFactory.getLogger(AeronPubSubTest.class);
	@Test
	void testStart() {
		AeronPublisher pub = new AeronPublisher(30001, 1001);
		assert !pub.isStarted();
		pub.start();
		assert pub.isStarted();
		pub.stop();
	}

	@Test
	void testPublishNotConnected() throws Exception{
		final String MSG = "msg";
		AeronPublisher pub = new AeronPublisher(30002, 1001);
		pub.start();
		long position = pub.publish(MSG);
		log.info("position: {}", position);			
		pub.stop();
		assert position == -1L;
	}
	
	@Test
	void testSubscribeNoPublisher() throws Exception{
		AeronSubscriber sub = new AeronSubscriber(30003, 1001, s->{});
		sub.start();
		sub.stop();
	}
	
	@Test
	void testPubSub1to1() throws InterruptedException {
		final String MSG = "msg";
		AeronPublisher pub = new AeronPublisher(30004, 1002);
		pub.start();
		List<String> msgReceived = new ArrayList<>(1);
		AeronSubscriber sub = new AeronSubscriber(30004, 1002, s->{log.info("sub received: {}", s); msgReceived.add(s);});
		sub.start();
		Thread.sleep(1000);
		long position = pub.publish(MSG);
		assert position > 0;
		Thread.sleep(1000);
		assert MSG.equals(msgReceived.get(0));
		Thread.sleep(1000);
		sub.stop();
		pub.stop();
	}
	
	@Test
	void testPubSub1toN() throws InterruptedException {
		final String MSG = "msg";
		AeronPublisher pub = new AeronPublisher(30005, 1003);
		pub.start();
		int n = 10;
		List<String> msgReceived = new ArrayList<>(n);
		
		AeronSubscriber[] subs = new AeronSubscriber[n];
		for (int i = 0; i < n; i++) {		
			subs[i] = new AeronSubscriber(30005, 1003, s->{log.info("sub received: {}", s); msgReceived.add(s);});
			subs[i].start();
		}
		
		Thread.sleep(1000);
		long position = pub.publish(MSG);
		assert position > 0;
		Thread.sleep(1000);
		for (int i = 0; i < n; i++) {
			assert MSG.equals(msgReceived.get(i));
		}
		
		Thread.sleep(1000);
		for (int i = 0; i < n; i++) {
			subs[i].stop();
		}
		pub.stop();
	}
	
}

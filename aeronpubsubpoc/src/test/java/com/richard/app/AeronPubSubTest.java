package com.richard.app;

import org.junit.jupiter.api.Test;

class AeronPubSubTest {

	@Test
	void testStart() {
		AeronPublisher pub = new AeronPublisher(30001, 1001);
		assert !pub.isStarted();
		pub.start();
		assert pub.isStarted();
		pub.stop();
	}

	@Test
	void testPublish() {
		final String MSG = "msg";
		Thread pubt = new Thread(()->{
			AeronPublisher pub = new AeronPublisher(30002, 1001);
			pub.start();
			long position = pub.publish(MSG);
			assert position > 0;
			pub.stop();
		}, "testPublish-pubt");
		pubt.start();
	}
	
	@Test
	void testPubSub1to1() {
		final String MSG = "msg";
		AeronSubscriber sub = new AeronSubscriber(3003, 1001, s->{assert MSG.equals(s);});
		sub.start();
		Thread pubt = new Thread(()->{
			AeronPublisher pub = new AeronPublisher(30003, 1001);
			pub.start();
			long position = pub.publish(MSG);
			assert position > 0;
			pub.stop();
		}, "testPubSub1to1-pubt");
		pubt.start();
		sub.stop();
		
	}
}

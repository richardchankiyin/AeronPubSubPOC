package com.richard.app;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.aeron.logbuffer.FragmentHandler;

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
		AeronSubscriber sub = new AeronSubscriber(30004, 1002, s->{log.info("testPubSub1to1 - sub received: {}", s); msgReceived.add(s);});
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
		final int n = 10;
		List<String> msgReceived = new ArrayList<>(n);
		
		AeronSubscriber[] subs = new AeronSubscriber[n];
		for (int i = 0; i < n; i++) {		
			subs[i] = new AeronSubscriber(30005, 1003, s->{log.info("testPubSub1toN - sub received: {}", s); msgReceived.add(s);});
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
	
	@Test
	void testPubSubMultipleMessages() throws InterruptedException {
		final String MSG = "msg";
		AeronPublisher pub = new AeronPublisher(30006, 1004);
		pub.start();
		List<String> msgReceived = new ArrayList<>(1);
		AeronSubscriber sub = new AeronSubscriber(30006, 1004, s->{log.info("testPubSubMultipleMessages - sub received: {}", s); msgReceived.add(s);});
		sub.start();
		Thread.sleep(1000);
		final int n = 10;
		for (int i = 0; i < n; i++) {
			long position = pub.publish(MSG + i);
			assert position > 0;
		}
		Thread.sleep(1000);
		for (int i = 0; i < n; i++) {
			assert (MSG + i).equals(msgReceived.get(i));
		}
		Thread.sleep(1000);
		sub.stop();
		pub.stop();
	}
	
	@Test
	void testPubFirstThenSubStart() throws InterruptedException {
		final String MSG = "msg";
		final String MSG2 = "msg2";
		AeronPublisher pub = new AeronPublisher(30007, 1005);
		pub.start();
		
		long position = pub.publish(MSG);
		assert position == -1L;
		
		List<String> msgReceived = new ArrayList<>(1);
		AeronSubscriber sub = new AeronSubscriber(30007, 1005, s->{log.info("testPubFirstThenSubStart - sub received: {}", s); msgReceived.add(s);});
		sub.start();
		
		Thread.sleep(1000);
		
		position = pub.publish(MSG2);
		assert position > 0;
		
		Thread.sleep(1000);
		
		assert msgReceived.size() == 1;
		assert MSG2.equals(msgReceived.get(0));
		
		sub.stop();
		pub.stop();
	}
	
	@Test
	void testArchiverStart() throws InterruptedException {
		AeronArchiver archiver = new AeronArchiver("localhost", 30008, 31008);
		archiver.onStart();
		
		assert State.ARCHIVE_READY == archiver.getState();
		
		long position = archiver.appendMsg("testing");
		
		assert position > 0;
		
		archiver.onClose();
		
	}
	
	
	@Test
	void testArchiverStartAndSubscribe() throws InterruptedException {
		AeronArchiver archiver = new AeronArchiver("localhost", 30009, 31009);
		archiver.onStart();
		
		assert State.ARCHIVE_READY == archiver.getState();
		
		long position = archiver.appendMsg("testing");
		
		assert position > 0;
		
		
		AeronArchiveSubcriberFragmentHandler handler = new AeronArchiveSubcriberFragmentHandler();
		
		AeronArchiveSubscriber subscriber = new AeronArchiveSubscriber("localhost","localhost",30009,31009,32009,handler);
		
		subscriber.connect();
		
		subscriber.onStart();
		
		archiver.appendMsg("testing2");
		
		int i = 0;
		while (i < 25) {
			log.info("i: {}", i);
			subscriber.doWork();
			Thread.sleep(10);
			i++;
		}
		
		assert handler.getCount() == 2;
		
		subscriber.onClose();
	}
	
	
	@Disabled
	@Test
	void testReplayMergeSubscribe() throws InterruptedException {
		AeronPublisher pub = new AeronPublisher("aeron:ipc", 100);
		AeronArchiver archiver = new AeronArchiver("localhost", 41001, 42001);
		pub.start();
		archiver.onStart();
		pub.publish("testing");
		archiver.appendMsg("testing");
		Thread.sleep(1000);
		AeronArchiveReplayMergeSubscriber sub = new AeronArchiveReplayMergeSubscriber("aeron:udp?control-mode=manual|control=localhost:40001", "localhost", "localhost", 41001,
		       42001, 0, new AeronArchiveSubcriberFragmentHandler());
		
		sub.onStart();
		int i = 0;
		while (i < 25) {
			log.info("i: {}", i);
			sub.doWork();
			Thread.sleep(10);
			i++;
		}
		
		
		
		Thread.sleep(1000);

		pub.stop();
		archiver.onClose();
	}
	
	
	@Disabled
	@Test
	void testArchivablePubNotConnected() {
		final String DIR = "archive/testArchivablePubNotConnected";
		File dir = new File(DIR);
		dir.mkdirs();
		final String MSG = "msg";
		//AeronPublisher pub = new AeronArchivablePublisher(30008, 31008,32008,33008, 1006, DIR);
		AeronPublisher pub = new AeronArchivablePublisher(31008,32008,33008, 1006, DIR);
		pub.start();
		long position = pub.publish(MSG);
		log.info("position: {}", position);			
		pub.stop();
		assert position == -1L;
	}
}

package com.richard.app.sample;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


class ArchiveHostAgentTest {
	private static final Logger log = LoggerFactory.getLogger(ArchiveHostAgentTest.class);
	@Test
	void testLaunchAgentAndSubscribe() throws InterruptedException {

		
		ArchiveHostAgent agent = new ArchiveHostAgent("localhost",17000, 17001);
		agent.onStart();
		assert State.AERON_READY == agent.getState();
		int retcde = agent.doWork();
		assert 0 == retcde;
		assert State.ARCHIVE_READY == agent.getState();
		

		Thread.sleep(1000);
		agent.doWork();
		Thread.sleep(1000);
		agent.doWork();

		assert State.ARCHIVE_READY == agent.getState();
		
		ArchiveClientFragmentHandler handle = new ArchiveClientFragmentHandler();
		ArchiveClientAgent clientagent = new ArchiveClientAgent("localhost", "localhost", 17000, 17001, handle);

		clientagent.onStart();
		Thread.sleep(1000);
		clientagent.doWork();
		Thread.sleep(1000);
		clientagent.doWork();
		Thread.sleep(1000);
		clientagent.doWork();
		Thread.sleep(2000);
		
		clientagent.onClose();
		
		agent.onClose();		
	}
	
	

}

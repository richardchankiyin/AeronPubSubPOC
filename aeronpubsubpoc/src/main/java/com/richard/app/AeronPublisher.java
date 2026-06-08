package com.richard.app;


import io.aeron.Aeron;
import io.aeron.Publication;
import io.aeron.driver.MediaDriver;
import org.agrona.BufferUtil;
import org.agrona.concurrent.UnsafeBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



public class AeronPublisher {
	private static final Logger log = LoggerFactory.getLogger(AeronPublisher.class);
	private Aeron aeron = null;
	private final Aeron.Context ctx;
	private Publication publication = null;
	private MediaDriver driver = null;
	private String channel = null;
	private int streamid;
	
	private boolean isStarted = false;
	private UnsafeBuffer buffer;
	
	private AeronPublisher(String channel, int streamid) {
		this.ctx = new Aeron.Context();
		this.channel = channel;
		this.streamid = streamid;
		log.info("channel: {} streamid: {}", this.channel, this.streamid);
	}
	
	public AeronPublisher(int port, int streamid) {
		this("localhost", port, streamid);
	}
	
	public AeronPublisher(String host, int port, int streamid) {
		this("aeron:udp?control-mode=dynamic|control=" + host + ":" + port, streamid);
	}
	
	
	protected MediaDriver getMediaDriver() {
		final MediaDriver.Context driverCtx = new MediaDriver.Context()
				.spiesSimulateConnection(true).dirDeleteOnStart(true).dirDeleteOnShutdown(true);
		return MediaDriver.launchEmbedded(driverCtx);
	}
	
	public void start() {
		//TODO to be changed to external later
		
		this.driver = getMediaDriver();
		this.ctx.aeronDirectoryName(this.driver.aeronDirectoryName());
		this.aeron = Aeron.connect(this.ctx);
		this.publication = aeron.addPublication(this.channel, this.streamid);
		this.buffer = new UnsafeBuffer(BufferUtil.allocateDirectAligned(256, 64));
		this.isStarted = true;
		log.info("started!");
	}
	
	public boolean isStarted() {
		return this.isStarted;
	}
	
	
	public long publish(String msg) {
		try {
			final int length = this.buffer.putStringWithoutLengthAscii(0, msg);
			log.debug("msg: {} length: {}", msg, length);
			return this.publication.offer(buffer, 0, length);
		}
		catch (NullPointerException npe) {
			if (!isStarted) {
				this.start();
				return this.publication.offer(buffer, 0, this.buffer.putStringAscii(0, msg));
			}
			return -999;
		} catch (Exception e) {
			log.error("publish error: ", e);
			return -9999;
		}
	}

	public void stop() {
		if (this.publication != null) this.publication.close();
		if (this.aeron != null) this.aeron.close();
		if (this.driver != null) this.driver.close();
		this.isStarted = false;
	}
	
}

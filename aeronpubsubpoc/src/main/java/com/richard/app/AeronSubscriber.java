package com.richard.app;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.YieldingIdleStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.aeron.Aeron;
import io.aeron.FragmentAssembler;
import io.aeron.Subscription;
import io.aeron.driver.MediaDriver;
import io.aeron.logbuffer.FragmentHandler;

public class AeronSubscriber {
	private static final Logger log = LoggerFactory.getLogger(AeronSubscriber.class);
	private MediaDriver driver = null;
	private Aeron aeron = null;
	private Aeron.Context ctx = null;
	private Subscription subscription = null;
	private String channel;
	private int streamid;
	private Consumer<String> msgHandler;
	private Thread subscriptionThread = null;
	final AtomicBoolean running;
	private static final int FRAGMENT_COUNT_LIMIT = 10;
	private AeronSubscriber(String channel, int streamid, Consumer<String> msgHandler) {
		this.ctx = new Aeron.Context();
		this.channel = channel;
		this.streamid = streamid;
		this.msgHandler = msgHandler;
		this.running = new AtomicBoolean(false);
	}
	
	public AeronSubscriber(int port, int streamid, Consumer<String> msgHandler) {
		this("localhost", port, streamid, msgHandler);
	}
	
	public AeronSubscriber(String host, int port, int streamid, Consumer<String> msgHandler) {
		this("aeron:udp?endpoint=" + host + ":" + port, streamid, msgHandler);
	}

	public void start() {
		if (this.running.getAndSet(true) == false) {
			this.driver = MediaDriver.launchEmbedded();
			this.aeron = Aeron.connect(this.ctx);
			this.subscription = aeron.addSubscription(this.channel, this.streamid);
			this.subscriptionThread = new Thread(()-> {
				subscriberLoop(getHandler(this.msgHandler), FRAGMENT_COUNT_LIMIT, this.running, new YieldingIdleStrategy()).accept(subscription);

			}, this.toString() + "-subscriptionThread");
			this.subscriptionThread.start();
		} else {
			log.warn("already started!");
		}
		
	}
	
    public static FragmentHandler getHandler(Consumer<String> msgHandler)
    {
        return (buffer, offset, length, header) ->
        {
            final String msg = buffer.getStringWithoutLengthAscii(offset, length);
            log.debug("session: {} msg received: {}", header.sessionId(), msg);
            msgHandler.accept(msg);
        };
    }
	
	
    private static Consumer<Subscription> subscriberLoop(
            final FragmentHandler fragmentHandler,
            final int limit,
            final AtomicBoolean running,
            final IdleStrategy idleStrategy)
    {
            return
                (subscription) ->
                {
                    final FragmentAssembler assembler = new FragmentAssembler(fragmentHandler);
                    while (running.get())
                    {
                        final int fragmentsRead = subscription.poll(assembler, limit);
                        idleStrategy.idle(fragmentsRead);
                    }
                };
    }
	
	public void stop() {
		this.running.set(false);
		try {
			// wait for subscription thread to finish
			if (this.subscriptionThread != null && this.subscriptionThread.isAlive()) {
				this.subscriptionThread.join(1000);
			}
		} catch (InterruptedException ie) {
			log.warn("thread issue ", ie);
		} finally {
			if (this.subscription != null) this.subscription.close();
			if (this.aeron != null) this.aeron.close();
			if (this.driver != null) this.driver.close();
		}
		
	}
	
}

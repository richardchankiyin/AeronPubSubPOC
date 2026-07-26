package com.richard.app;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Enumeration;

import org.agrona.CloseHelper;
import org.agrona.collections.MutableLong;
import org.agrona.concurrent.EpochClock;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.SleepingMillisIdleStrategy;
import org.agrona.concurrent.SystemEpochClock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.richard.app.sample.State;

import io.aeron.Aeron;
import io.aeron.Subscription;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.RecordingDescriptorConsumer;
import io.aeron.archive.client.ReplayMerge;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.exceptions.TimeoutException;
import io.aeron.logbuffer.FragmentHandler;

public class AeronArchiveReplayMergeSubscriber {

    public static final String AERON_UDP_ENDPOINT = "aeron:udp?endpoint=";
    private static final int RECORDED_STREAM_ID = 100;
    private static final int REPLAY_STREAM_ID = 200;
    private static final Logger LOGGER = LoggerFactory.getLogger(AeronArchiveReplayMergeSubscriber.class);
    private final String liveDestination;
    private long recordingId = Long.MIN_VALUE;
    private final String archiveHost;
    private final MediaDriver mediaDriver;
    private final String thisHost;
    private final int archiveControlPort;
    private final int archiveEventPort;
    private final int replayPort;
    private final FragmentHandler fragmentHandler;
    private final Aeron aeron;
    private final IdleStrategy idleStrategy;
    private AeronArchive archive;
    private AeronArchive.AsyncConnect asyncConnect;
    private State currentState;
    private Subscription replayDestinationSubs;
    private volatile boolean isRunning = false;

    public AeronArchiveReplayMergeSubscriber(final String liveDestination, final String archiveHost, final String thisHost, final int archiveControlPort,
        final int archiveEventPort, int replayPort, final FragmentHandler fragmentHandler)
    {
    	this.liveDestination = liveDestination;
        this.archiveHost = archiveHost;
        this.thisHost = localHost(thisHost);
        this.archiveControlPort = archiveControlPort;
        this.archiveEventPort = archiveEventPort;
        this.replayPort = replayPort;
        this.fragmentHandler = fragmentHandler;
        this.idleStrategy = new SleepingMillisIdleStrategy(250);

        LOGGER.info("launching media driver");
        //launch a media driver
        this.mediaDriver = MediaDriver.launch(new MediaDriver.Context()
            .dirDeleteOnStart(true)
            .threadingMode(ThreadingMode.SHARED)
            .sharedIdleStrategy(new SleepingMillisIdleStrategy()));

        LOGGER.info("connecting aeron; media driver directory {}", mediaDriver.aeronDirectoryName());
        //connect an aeron client
        this.aeron = Aeron.connect(new Aeron.Context()
            .aeronDirectoryName(mediaDriver.aeronDirectoryName())
            .idleStrategy(new SleepingMillisIdleStrategy()));

        this.currentState = State.AERON_READY;
    }

    public void connect() {
    	switch (currentState)
        {
            case AERON_READY -> connectToArchive();
            default -> LOGGER.error("current state not ready:  {}", currentState);
        }
    }
    
    //@Override
    public int doWork()
    {
        switch (currentState)
        {
            case AERON_READY -> connectToArchive();
            case POLLING_SUBSCRIPTION -> runReplayMerge();
            default -> LOGGER.error("unknown state {}", currentState);
        }

        return 0;
    }

    private void runReplayMerge() {
    	this.isRunning = true;
    	final var localReplayChannelEphemeral = AERON_UDP_ENDPOINT + thisHost + ":" + this.replayPort;
    	final var replayDestination = AERON_UDP_ENDPOINT + thisHost + ":0";
    	long startPosition = 0L;
    	final long mergeTimeoutMs = 5000L;
    	final EpochClock epochClock = new SystemEpochClock();
    	final Subscription subscription = aeron.addSubscription(this.liveDestination, 1001);
    	subscription.addDestination(replayDestination);
    	
    	try (ReplayMerge replayMerge = new ReplayMerge(subscription, this.archive, replayDestination, localReplayChannelEphemeral, this.liveDestination, recordingId, startPosition, epochClock, mergeTimeoutMs)) {
    		// 4. Duty cycle loop
    	    while (!replayMerge.isMerged()) {
    	        int fragments = replayMerge.poll(fragmentHandler, 10);
    	        LOGGER.info("merging from replayMerge poll with fragments: {}", fragments);
    	        // Handle idle strategy if no fragments were read
    	        // ...
    	        
    	        if (replayMerge.hasFailed()) {
    	            throw new RuntimeException("ReplayMerge failed");
    	        }
    	    }
    	    
    	    // 5. Once merged, continue polling the live stream via the base subscription
    	    while (this.isRunning) {
    	        int fragments = subscription.poll(fragmentHandler, 10);
    	        LOGGER.info("merging from subscription poll with fragments: {}", fragments);
    	        // ...
    	    }
    		
    	}
    	
    	
    }
    
    
    private void connectToArchive()
    {
        //start an asyncConnect if one not in progress
        if (asyncConnect == null)
        {
            LOGGER.info("connecting aeron archive");
            final String controlRequestChannel = AERON_UDP_ENDPOINT + archiveHost + ":" + archiveControlPort;
            final String controlResponseChannel = AERON_UDP_ENDPOINT + thisHost + ":0";
            final String archivingEventsChannel = AERON_UDP_ENDPOINT + archiveHost + ":" + archiveEventPort;
            asyncConnect = AeronArchive.asyncConnect(new AeronArchive.Context()
                .controlRequestChannel(controlRequestChannel)
                .recordingEventsChannel(archivingEventsChannel)
                .controlResponseChannel(controlResponseChannel)
                .aeron(aeron));
            
            LOGGER.debug("controlRequestChannel: {} | controlResponseChannel: {} | archivingEventsChannel: {}", controlRequestChannel, controlResponseChannel, archivingEventsChannel);
        }
        else
        {
            //if the archive hasn't been set yet, poll it after idling 250ms
            if (null == archive)
            {
                LOGGER.info("awaiting aeron archive");
                idleStrategy.idle();
                try
                {
                    archive = asyncConnect.poll();
                }
                catch (final TimeoutException e)
                {
                    LOGGER.info("timeout");
                    asyncConnect = null;
                }
            }
            else
            {
                LOGGER.info("finding remote recording");
                //archive is connected. find the recording on the remote archive host
                recordingId = getRecordingId("aeron:ipc", RECORDED_STREAM_ID);
                if (recordingId != Long.MIN_VALUE)
                {
                    //ask aeron to assign an ephemeral port for this replay
                    final var localReplayChannelEphemeral = AERON_UDP_ENDPOINT + thisHost + ":" + this.replayPort;
                    //construct a local subscription for the remote host to replay to
                    replayDestinationSubs = aeron.addSubscription(localReplayChannelEphemeral, REPLAY_STREAM_ID);
                    //resolve the actual port and use that for the replay
                    final var actualReplayChannel = replayDestinationSubs.tryResolveChannelEndpointPort();
                    LOGGER.info("actualReplayChannel={}", actualReplayChannel);
                    //replay from the archive recording the start
                    final long replaySession =
                        archive.startReplay(recordingId, 0L, Long.MAX_VALUE, actualReplayChannel, REPLAY_STREAM_ID);
                    LOGGER.info("ready to poll subscription, replaying to {}, image is {}", actualReplayChannel,
                        (int)replaySession);
                    currentState = State.POLLING_SUBSCRIPTION;
                }
                else
                {
                    //await the remote host being ready, idle 250ms
                    idleStrategy.idle();
                }
            }
        }
    }

    private long getRecordingId(final String remoteRecordedChannel, final int remoteRecordedStream)
    {
        final var lastRecordingId = new MutableLong();
        final RecordingDescriptorConsumer consumer = (controlSessionId, correlationId, recordingId,
            startTimestamp, stopTimestamp, startPosition,
            stopPosition, initialTermId, segmentFileLength,
            termBufferLength, mtuLength, sessionId,
            streamId, strippedChannel, originalChannel,
            sourceIdentity) -> lastRecordingId.set(recordingId);

        final var fromRecordingId = 0L;
        final var recordCount = 100;

        final int foundCount = archive.listRecordingsForUri(fromRecordingId, recordCount, remoteRecordedChannel,
            remoteRecordedStream, consumer);

        if (0 == foundCount)
        {
            return Long.MIN_VALUE;
        }

        return lastRecordingId.get();
    }


    public String roleName()
    {
        return "archive-client";
    }


    public void onStart()
    {
        //Agent.super.onStart();
        LOGGER.info("starting");
    }

    public static String localHost(final String fallback)
    {
        try
        {
            final Enumeration<NetworkInterface> interfaceEnumeration = NetworkInterface.getNetworkInterfaces();
            while (interfaceEnumeration.hasMoreElements())
            {
                final var networkInterface = interfaceEnumeration.nextElement();

                if (networkInterface.getName().startsWith("eth0"))
                {
                    final Enumeration<InetAddress> interfaceAddresses = networkInterface.getInetAddresses();
                    while (interfaceAddresses.hasMoreElements())
                    {
                        if (interfaceAddresses.nextElement() instanceof Inet4Address inet4Address)
                        {
                            LOGGER.info("detected ip4 address as {}", inet4Address.getHostAddress());
                            return inet4Address.getHostAddress();
                        }
                    }
                }
            }
        }
        catch (final SocketException e)
        {
            LOGGER.info("Failed to get address");
        }
        return fallback;
    }


    public void onClose()
    {
        //Agent.super.onClose();
        LOGGER.info("shutting down");
        this.isRunning = false;
        CloseHelper.quietClose(replayDestinationSubs);
        CloseHelper.quietClose(archive);
        CloseHelper.quietClose(aeron);
        CloseHelper.quietClose(mediaDriver);
    }
}

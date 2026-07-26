package com.richard.app.sample;


import io.aeron.Aeron;
import io.aeron.CommonContext;
import io.aeron.FragmentAssembler;
import io.aeron.Image;
import io.aeron.Subscription;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.ReplayMerge;
import io.aeron.driver.MediaDriver;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.ShutdownSignalBarrier;
import org.agrona.concurrent.YieldingIdleStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;


/**
 * This is an Aeron subscriber utilising {@link io.aeron.archive.client.ReplayMerge}.
 * <p>
 * The application uses {@code ReplayMerge} to replay from a recording, before joining the live stream.
 * It uses the default channel for live, and the default stream ID for both live and replay.
 * These defaults can be overwritten by setting their corresponding Java system properties
 * at the command line, for example:
 * export JVM_OPTS="-Daeron.sample.channel=aeron:udp?endpoint=localhost:5555 -Daeron.sample.streamId=20"
 */
public class ReplayMergeSubscriber
{
	private static final Logger LOGGER = LoggerFactory.getLogger(ReplayMergeSubscriber.class);
    //private static final int STREAM_ID = 100;
    //private static final String LIVE_DESTINATION = SampleConfiguration.CHANNEL;
    //private static final String REPLAY_DESTINATION = "aeron:udp?endpoint=localhost:0";
    //private static final String MDS_CHANNEL = "aeron:udp?control-mode=manual";
    //private static final int FRAGMENT_COUNT_LIMIT = SampleConfiguration.FRAGMENT_COUNT_LIMIT;
    //private static final boolean EMBEDDED_MEDIA_DRIVER = SampleConfiguration.EMBEDDED_MEDIA_DRIVER;

    /**
     * Main method for launching the process.
     *
     * @param args passed to the process.
     */
    @SuppressWarnings("try")
    public static void subscribe(int streamID, String liveDestination, String replayDestination, String controlRequestChannel, String controlResponseChannel, String mdsChannel, int fragmentCountLimit, boolean isEmbeddedMediaDriver)
    {
    	LOGGER.info("Subscribing to live {}, and replay {} on stream id {} ", liveDestination, replayDestination,streamID);

        final AtomicBoolean running = new AtomicBoolean(true);
        final AtomicBoolean isLive = new AtomicBoolean(false);
        final IdleStrategy idleStrategy = new YieldingIdleStrategy();
        final FragmentHandler fragmentHandler = new FragmentAssembler(
            (final DirectBuffer buffer, final int offset, final int length, final Header header) ->
            {
                final String msg = buffer.getStringWithoutLengthAscii(offset, length);
                final String streamState = isLive.get() ? "live" : "replay";

                LOGGER.info("Message to {} stream {} from session {} ({}@{}) <<{}>>{}",
                    streamState, streamID, header.sessionId(), length, offset, msg);
            });

        try (ShutdownSignalBarrier barrier = new ShutdownSignalBarrier(() -> running.set(false));
            MediaDriver driver = isEmbeddedMediaDriver ?
                MediaDriver.launchEmbedded(new MediaDriver.Context().terminationHook(barrier::signalAll)) : null)
        {
            final Aeron.Context ctx = new Aeron.Context()
                .availableImageHandler(ReplayMergeSubscriber::printAvailableImage)
                .unavailableImageHandler(ReplayMergeSubscriber::printUnavailableImage);

            if (isEmbeddedMediaDriver)
            {
                ctx.aeronDirectoryName(driver.aeronDirectoryName());
            }

            final AeronArchive.Context aeronArchiveCtx = new AeronArchive.Context().controlRequestChannel(controlRequestChannel).controlResponseChannel(controlResponseChannel);

            // Create Aeron and AeronArchive instances using the configured Context.
            try (Aeron aeron = Aeron.connect(ctx);
                AeronArchive aeronArchive = AeronArchive.connect(aeronArchiveCtx.aeron(aeron)))
            {
                if (null == aeronArchive)
                {
                	LOGGER.error("Could not connect to aeron archive.");
                    return;
                }

                final RecordingDescriptor descriptor = findLatestRecording(aeronArchive, liveDestination, streamID);

                if (descriptor == null)
                {
                	LOGGER.error("No recordings found for channel {} with stream id {}", liveDestination, streamID);
                    return;
                }
                final String replayChannel = CommonContext.UDP_CHANNEL + "?session-id=" + descriptor.sessionId();
                final String subscriptionChannel = mdsChannel + "|session-id=" + descriptor.sessionId();

                // Create a Multi-Destination Subscription on the Aeron instance for the ReplayMerge instance, then
                // create a ReplayMerge instance.
                try (Subscription subscription = aeron.addSubscription(subscriptionChannel, streamID);
                    ReplayMerge replayMerge = new ReplayMerge(
                        subscription,
                        aeronArchive,
                        replayChannel,
                        replayDestination,
                        liveDestination,
                        descriptor.recordingId(),
                        descriptor.startPosition()))
                {
                    while (running.get())
                    {
                        if (replayMerge.hasFailed())
                        {
                            throw new IllegalStateException("ReplayMerge has failed, " + replayMerge);
                        }

                        if (replayMerge.isMerged() && !isLive.get())
                        {
                        	LOGGER.info("===========");
                        	LOGGER.info("ReplayMerge has joined live stream.");
                        	LOGGER.info("===========");
                            isLive.set(true);
                        }

                        final int fragments = replayMerge.poll(fragmentHandler, fragmentCountLimit);

                        idleStrategy.idle(fragments);
                    }
                    LOGGER.info("Shutting down...");
                }
            }
        }
    }
    
    private static void printAvailableImage(final Image image)
    {
        final Subscription subscription = image.subscription();
        LOGGER.info(
            "Available image on {} streamId={} sessionId={} mtu={} term-length={} from {}{}",
            subscription.channel(), subscription.streamId(), image.sessionId(), image.mtuLength(),
            image.termBufferLength(), image.sourceIdentity());
    }

    /**
     * Print the information for an unavailable image to stdout.
     *
     * @param image that has gone inactive.
     */
    private static void printUnavailableImage(final Image image)
    {
        final Subscription subscription = image.subscription();
        LOGGER.warn(
            "Unavailable image on {} streamId={} sessionId={}{}",
            subscription.channel(), subscription.streamId(), image.sessionId());
    }
    private static RecordingDescriptor findLatestRecording(
            final AeronArchive aeronArchive,
            final String channelFragment,
            final int streamId
       )
       {
           final RecordingDescriptorCollector collector = new RecordingDescriptorCollector(1);

           if (0 == aeronArchive.listRecordingsForUri(
               0,
               Integer.MAX_VALUE,
               channelFragment,
               streamId,
               collector.reset())
           )
           {
               return null;
           }

           final int lastIndex = collector.descriptors().size() - 1;
           return collector.descriptors().get(lastIndex);
       }
}
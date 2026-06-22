package com.richard.app.sample;

import org.agrona.concurrent.AgentRunner;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.ShutdownSignalBarrier;
import org.agrona.concurrent.SleepingMillisIdleStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ArchiveHost
{
    private static final Logger LOGGER = LoggerFactory.getLogger(ArchiveHost.class);

    public static void main(final String[] args)
    {
        final var archiveHost = System.getProperty("ARCHIVEHOST");
        final var controlPort = System.getProperty("CONTROLPORT");
        final var eventsPort = System.getProperty("EVENTSPORT");

        if (archiveHost == null || controlPort == null || eventsPort == null)
        {
            LOGGER.error("requires 3 env vars: ARCHIVEHOST, CONTROLPORT, EVENTSPORT");
        }
        else
        {
            final var controlChannelPort = Integer.parseInt(controlPort);
            final var recEventsChannelPort = Integer.parseInt(eventsPort);
            final var hostAgent = new ArchiveHostAgent(archiveHost, controlChannelPort, recEventsChannelPort);
            final IdleStrategy idleStrategy = new SleepingMillisIdleStrategy();
            try (var barrier = new ShutdownSignalBarrier();
                var runner = new AgentRunner(idleStrategy, ArchiveHost::errorHandler, null, hostAgent))
            {
                AgentRunner.startOnThread(runner);

                barrier.await();
            }
        }
    }

    private static void errorHandler(final Throwable throwable)
    {
        LOGGER.error("agent failure {}", throwable.getMessage(), throwable);
    }
}
package com.richard.app.sample;

import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;

import java.util.concurrent.atomic.AtomicLong;

import org.agrona.DirectBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ArchiveClientFragmentHandler implements FragmentHandler
{
    private static final Logger LOGGER = LoggerFactory.getLogger(ArchiveClientFragmentHandler.class);
    
    private AtomicLong count = new AtomicLong(0);

    @Override
    public void onFragment(final DirectBuffer buffer, final int offset, final int length, final Header header)
    {
        final var read = buffer.getLong(offset);
        count.addAndGet(1);
        LOGGER.info("received {}", read);
    }
    
    public long getCount() { return count.get(); }
}

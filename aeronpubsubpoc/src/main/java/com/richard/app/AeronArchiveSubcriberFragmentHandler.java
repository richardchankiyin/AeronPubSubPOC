package com.richard.app;

import java.util.concurrent.atomic.AtomicLong;

import org.agrona.DirectBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;

public class AeronArchiveSubcriberFragmentHandler implements FragmentHandler
{
    private static final Logger LOGGER = LoggerFactory.getLogger(AeronArchiveSubcriberFragmentHandler.class);
    
    private AtomicLong count = new AtomicLong(0);

    @Override
    public void onFragment(final DirectBuffer buffer, final int offset, final int length, final Header header)
    {
        final var read = buffer.getStringWithoutLengthAscii(offset, length);
        count.addAndGet(1);
        LOGGER.info("received {}", read);
    }
    
    public long getCount() { return count.get(); }
}

package com.richard.app;

import java.io.File;

import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.YieldingIdleStrategy;
import org.agrona.concurrent.status.CountersReader;

import io.aeron.archive.Archive;
import io.aeron.archive.ArchivingMediaDriver;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.codecs.SourceLocation;
import io.aeron.archive.status.RecordingPos;
import io.aeron.driver.MediaDriver;


public class AeronArchivablePublisher extends AeronPublisher {

	private ArchivingMediaDriver archivingMediaDriver = null;
	private AeronArchive aeronArchive = null;
	private File archiveDirLocation;
	private IdleStrategy archivablePublishIdleStrategy = null;
	
	private void init(String archiveDirLocation) {
		this.archiveDirLocation = new File(archiveDirLocation);
		if (!this.archiveDirLocation.isDirectory()) {
			throw new IllegalArgumentException("archiveDirLocation: " + archiveDirLocation + " invalid");
		}
		this.archivablePublishIdleStrategy = new YieldingIdleStrategy();
	}
	
	public AeronArchivablePublisher(String host, int port, int streamid, String archiveDirLocation) {		
		super(host, port, streamid);
		init(archiveDirLocation);
	}
	
	public AeronArchivablePublisher(int port, int streamid, String archiveDirLocation) {
		super(port, streamid);
		init(archiveDirLocation);
	}
	
	
	protected ArchivingMediaDriver getArchivingMediaDriver() {
		//TODO still facing issues in setting up
		return ArchivingMediaDriver.launch(
				new MediaDriver.Context().spiesSimulateConnection(true).dirDeleteOnStart(true), 
					new Archive.Context().deleteArchiveOnStart(true).archiveDir(this.archiveDirLocation).controlChannelEnabled(false));
	}
	
	
	public void start() {
		super.start();
		// perform archive connect
		
		this.archivingMediaDriver = getArchivingMediaDriver();
		this.aeronArchive = AeronArchive.connect(new AeronArchive.Context().aeron(getAeron()));
		this.aeronArchive.startRecording(getChannel(), getStreamid(), SourceLocation.LOCAL);
	}
	
	public long publish(String msg) {
		long result = super.publish(msg);
		
		
		// perform write to archive and only return when it is done
		final long stopPosition = getPublication().position();
		final CountersReader countersReader = getAeron().countersReader();
		final int counterId = RecordingPos.findCounterIdByRecording(countersReader, getPublication().sessionId(), -1);
		while (countersReader.getCounterValue(counterId) < stopPosition)
        {
			this.archivablePublishIdleStrategy.idle();
        }
		
		return result;
	}
	
	public void stop() {
		if (this.aeronArchive != null) this.aeronArchive.close();
		if (this.archivingMediaDriver != null) this.archivingMediaDriver.close();
		super.stop();
	}
	
}

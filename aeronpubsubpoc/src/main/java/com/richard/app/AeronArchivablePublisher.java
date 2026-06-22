package com.richard.app;

import java.io.File;

import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.YieldingIdleStrategy;
import org.agrona.concurrent.status.CountersReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.aeron.archive.Archive;
import io.aeron.archive.ArchivingMediaDriver;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.codecs.SourceLocation;
import io.aeron.archive.status.RecordingPos;
import io.aeron.driver.MediaDriver;


public class AeronArchivablePublisher extends AeronPublisher {
	private static final Logger log = LoggerFactory.getLogger(AeronArchivablePublisher.class);
	public static final String AERON_UDP_ENDPOINT = "aeron:udp?endpoint=";
	private ArchivingMediaDriver archivingMediaDriver = null;
	private AeronArchive aeronArchive = null;
	private File archiveDirLocation;
	private IdleStrategy archivablePublishIdleStrategy = null;
	private String controlRequestChannel = null;
	private String replicatingChannel = null;
	private String controlResponseChannel = null;
	private String recordingEventsChannel = null;
	
	private void init(String archiveDirLocation, String host, int controlChannelPort, int replicatingChannelPort, int recordingEventsPort) {
		this.controlRequestChannel = AERON_UDP_ENDPOINT + host + ":" + controlChannelPort; 
		this.controlResponseChannel = AERON_UDP_ENDPOINT + host + ":0";
		this.replicatingChannel = AERON_UDP_ENDPOINT + host + ":" + replicatingChannelPort;
		this.recordingEventsChannel =
		        "aeron:udp?control-mode=dynamic|control=" + host + ":" + recordingEventsPort;
		
		this.archiveDirLocation = new File(archiveDirLocation);
		if (!this.archiveDirLocation.isDirectory()) {
			throw new IllegalArgumentException("archiveDirLocation: " + archiveDirLocation + " invalid");
		}
		this.archivablePublishIdleStrategy = new YieldingIdleStrategy();
		
	}
	
	public AeronArchivablePublisher(String host, int port, int controlChannelPort, int replicatingChannelPort, int recordingEventsPort, int streamid, String archiveDirLocation) {		
		super(host, port, streamid);
		init(archiveDirLocation, host, controlChannelPort, replicatingChannelPort, recordingEventsPort);
	}
	
	public AeronArchivablePublisher(int port, int controlChannelPort, int replicatingChannelPort, int recordingEventsPort, int streamid, String archiveDirLocation) {
		super(port, streamid);
		init(archiveDirLocation, "localhost", controlChannelPort, replicatingChannelPort, recordingEventsPort);
	}
	
	
	public AeronArchivablePublisher(int controlChannelPort, int replicatingChannelPort, int recordingEventsPort, int streamid, String archiveDirLocation) {
		super("aeron:ipc", streamid);
		init(archiveDirLocation, "localhost", controlChannelPort, replicatingChannelPort, recordingEventsPort);
	}
	
	protected ArchivingMediaDriver getArchivingMediaDriver() {
		//TODO still facing issues in setting up
		return ArchivingMediaDriver.launch(
				new MediaDriver.Context().spiesSimulateConnection(true).dirDeleteOnStart(true), 
					new Archive.Context().deleteArchiveOnStart(true).archiveDir(this.archiveDirLocation)
						.controlChannel(this.controlRequestChannel).replicationChannel(this.replicatingChannel).recordingEventsChannel(this.recordingEventsChannel));
	}
	
	
	public void start() {
		super.start();
		// perform archive connect
		
		this.archivingMediaDriver = getArchivingMediaDriver();
		this.aeronArchive = AeronArchive.connect(new AeronArchive.Context().aeron(getAeron()).controlRequestChannel(this.controlRequestChannel)
				.controlResponseChannel(this.controlResponseChannel).recordingEventsChannel(this.recordingEventsChannel));
		
		this.getAeron().addExclusivePublication("aeron:ipc", getStreamid());
		this.aeronArchive.startRecording(getChannel(), getStreamid(), SourceLocation.LOCAL);
		final CountersReader countersReader = getAeron().countersReader();
		int counterId = RecordingPos.findCounterIdBySession(countersReader, getPublication().sessionId(), this.aeronArchive.archiveId()); 
		log.info("waiting for recording to start for session {} with archive id: {}", getPublication().sessionId(), this.aeronArchive.archiveId());
		while (CountersReader.NULL_COUNTER_ID == counterId)
	    {
			this.archivablePublishIdleStrategy.idle();
			log.debug("sleeping...");
	        counterId = RecordingPos.findCounterIdBySession(countersReader, getPublication().sessionId(), this.aeronArchive.archiveId());
	    }
		final long recordingId = RecordingPos.getRecordingId(countersReader, counterId);
		
		log.info("archive recording started; recording id is {}", recordingId);
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

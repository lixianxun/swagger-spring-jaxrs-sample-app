package com.wifi.tracksvc.process.batcher;

import java.util.List;

import com.ebay.jetstream.batcher.AutoFlushBatcher;

public class TrackSvcBatcher extends AutoFlushBatcher<FinderEvent> {

	public TrackSvcBatcher(int autoFlushSz, int flushInterval) {
		super(autoFlushSz, flushInterval);
	}
	
	@Override
	public void flush(List<FinderEvent> items) {
		//kfk processor send
	}
}

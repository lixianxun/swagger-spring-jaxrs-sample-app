package com.wifi.tracksvc.process;

import com.ebay.jetstream.util.RequestQueueProcessor;
import com.wifi.tracksvc.process.batcher.TrackSvcBatcher;
import com.wifi.tracksvc.request.FinderRequest;
import com.wifi.tracksvc.response.FinderResponse;

public class TrackSvcCommandHandler {

	private final RequestQueueProcessor requestProcessor;
	private final TrackSvcBatcher trkSvcBatcher;
	
	public TrackSvcCommandHandler() {
		requestProcessor = new RequestQueueProcessor(30000, 1, "track-svc-request-processor");
		trkSvcBatcher = new TrackSvcBatcher(50, 100);
	}
	
	public FinderResponse execute(FinderRequest req) {
		if(requestProcessor.processRequest(new FinderRequestTask(req))) {
			return null;
		}
		
		return null;
	}
	
	private class FinderRequestTask implements Runnable {
		private FinderRequest req;
		
		public FinderRequestTask(final FinderRequest req) {
			this.req = req;
		}

		@Override
		public void run() {
			//json array -> list
			//check list item
			//json -> avro GenericRecord --> FinderEvent
			
			try {
				trkSvcBatcher.write(null);
			} catch (Exception e) {
				//do it
			}
		}
	}
}

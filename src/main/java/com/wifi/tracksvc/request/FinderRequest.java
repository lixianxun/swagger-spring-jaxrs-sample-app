package com.wifi.tracksvc.request;

import lombok.Data;

@Data
public class FinderRequest {

	private byte[] keyBytes;
	private byte[] payload;
	
	private String subject;
	private int versionId;
	
	private String topic;
}

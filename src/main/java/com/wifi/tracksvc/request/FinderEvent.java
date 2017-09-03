package com.wifi.tracksvc.request;

import org.apache.avro.generic.GenericRecord;

import lombok.Data;

@Data
public class FinderEvent {

	private String schemaSubject;
	private int schemaVersionId;
	private int schemaUniqId;
	
	private GenericRecord record;
}

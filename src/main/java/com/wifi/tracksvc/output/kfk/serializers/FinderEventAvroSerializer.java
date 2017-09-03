package com.wifi.tracksvc.output.kfk.serializers;

import java.util.List;
import java.util.Map;

import org.apache.kafka.common.serialization.Serializer;

import com.wifi.tracksvc.request.FinderEvent;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;

public class FinderEventAvroSerializer extends AbstractKfkAvroSerializer implements Serializer<FinderEvent> {
	
	public FinderEventAvroSerializer(List<String> urls) {
		super(urls);
	}

	public FinderEventAvroSerializer(SchemaRegistryClient client) {
		super(client);
	}

	@Override
	public byte[] serialize(final String topic, FinderEvent recordWrapper) {
		return doSerialize(topic, recordWrapper.getRecord(), recordWrapper.getSchemaUniqId());
	}

	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {
	}

	@Override
	public void close() {

	}
}

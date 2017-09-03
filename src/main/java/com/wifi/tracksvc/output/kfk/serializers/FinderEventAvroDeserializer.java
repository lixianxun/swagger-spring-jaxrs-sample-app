package com.wifi.tracksvc.output.kfk.serializers;

import java.util.List;
import java.util.Map;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Deserializer;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;

public class FinderEventAvroDeserializer extends AbstractKfkAvroDeserializer implements Deserializer<GenericRecord> {
	
	public FinderEventAvroDeserializer(List<String> urls) {
		super(urls);
	}

	public FinderEventAvroDeserializer(SchemaRegistryClient client) {
		super(client);
	}

	@Override
	public GenericRecord deserialize(final String topic, final byte[] bytes) {
		return (GenericRecord)doDeserialize(topic, bytes);
	}

	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {
	}

	@Override
	public void close() {

	}
}

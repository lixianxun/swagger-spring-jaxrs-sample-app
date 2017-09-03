package com.wifi.tracksvc.output.kfk.serializers;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericContainer;
import org.springframework.util.Assert;

import com.wifi.tracksvc.output.kfk.serializers.exception.SerializationException;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;

public abstract class AbstractSRAwareKfkAvroSerDe {

	protected SchemaRegistryClient schemaRegistry;

	protected static final byte MAGIC_BYTE = 0x0;
	protected static final int ID_SIZE = 4;

	private static final Map<String, Schema> primitiveSchemas;

	static {
		Schema.Parser parser = new Schema.Parser();
		primitiveSchemas = new HashMap<>();
		primitiveSchemas.put("Null", createPrimitiveSchema(parser, "null"));
		primitiveSchemas.put("Boolean", createPrimitiveSchema(parser, "boolean"));
		primitiveSchemas.put("Integer", createPrimitiveSchema(parser, "int"));
		primitiveSchemas.put("Long", createPrimitiveSchema(parser, "long"));
		primitiveSchemas.put("Float", createPrimitiveSchema(parser, "float"));
		primitiveSchemas.put("Double", createPrimitiveSchema(parser, "double"));
		primitiveSchemas.put("String", createPrimitiveSchema(parser, "string"));
		primitiveSchemas.put("Bytes", createPrimitiveSchema(parser, "bytes"));
	}

	protected static Map<String, Schema> getPrimitiveSchemas() {
		return Collections.unmodifiableMap(primitiveSchemas);
	}

	public AbstractSRAwareKfkAvroSerDe(List<String> urls) {
		Assert.isTrue((null != urls && urls.size() > 0), "must provid schema registry urls");
		schemaRegistry = new CachedSchemaRegistryClient(urls, 1000);
	}

	public AbstractSRAwareKfkAvroSerDe(SchemaRegistryClient client) {
		Assert.notNull(client, "must provide a validate schema registry client");
		schemaRegistry = client;
	}

	public Schema getById(int id) {
		try {
			return schemaRegistry.getById(id);
		} catch (IOException | RestClientException e) {
			throw new SerializationException(e);
		}
	}

	public Schema getBySubjectAndId(String subject, int id) {
		try {
			return schemaRegistry.getBySubjectAndId(subject, id);
		} catch (IOException | RestClientException e) {
			throw new SerializationException(e);
		}
	}

	private static Schema createPrimitiveSchema(Schema.Parser parser, String type) {
		String schemaString = String.format("{\"type\" : \"%s\"}", type);
		return parser.parse(schemaString);
	}

	protected Schema getRecordSchema(Object object) {
		if (object == null) {
			return primitiveSchemas.get("Null");
		} else if (object instanceof Boolean) {
			return primitiveSchemas.get("Boolean");
		} else if (object instanceof Integer) {
			return primitiveSchemas.get("Integer");
		} else if (object instanceof Long) {
			return primitiveSchemas.get("Long");
		} else if (object instanceof Float) {
			return primitiveSchemas.get("Float");
		} else if (object instanceof Double) {
			return primitiveSchemas.get("Double");
		} else if (object instanceof CharSequence) {
			return primitiveSchemas.get("String");
		} else if (object instanceof byte[]) {
			return primitiveSchemas.get("Bytes");
		} else if (object instanceof GenericContainer) {
			return ((GenericContainer) object).getSchema();
		} else {
			throw new IllegalArgumentException(
					"Unsupported Avro type. Supported types are null, Boolean, Integer, Long, "
							+ "Float, Double, String, byte[] and IndexedRecord");
		}
	}
}
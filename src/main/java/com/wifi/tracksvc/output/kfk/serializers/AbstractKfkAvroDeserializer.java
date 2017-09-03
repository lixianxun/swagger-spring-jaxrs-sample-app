package com.wifi.tracksvc.output.kfk.serializers;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificRecord;
import org.springframework.util.Assert;

import com.wifi.tracksvc.output.kfk.serializers.exception.SerializationException;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;

public class AbstractKfkAvroDeserializer extends AbstractSRAwareKfkAvroSerDe {

	public static final String SCHEMA_REGISTRY_SCHEMA_VERSION_PROP = "schema.registry.schema.version";
	private final DecoderFactory decoderFactory = DecoderFactory.get();

	public AbstractKfkAvroDeserializer(List<String> urls) {
		super(urls);
	}

	public AbstractKfkAvroDeserializer(SchemaRegistryClient client) {
		super(client);
	}

	private ByteBuffer getByteBuffer(byte[] payload) {
		ByteBuffer buffer = ByteBuffer.wrap(payload);
		if (buffer.get() != MAGIC_BYTE) {
			throw new SerializationException("Unknown magic byte!");
		}
		return buffer;
	}

	protected Object doDeserialize(String topic, byte[] bytes) {
		return doDeserialize(topic, bytes, null);
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	protected Object doDeserialize(String topic, byte[] bytes, Schema readerSchema) {
		Assert.notNull(bytes, "bytes cant be null");
		int schemaUniqId = -1;
		try {
			ByteBuffer buffer = getByteBuffer(bytes);
			Schema writerSchema = schemaRegistry.getById(buffer.getInt());
			long eventSendTime = buffer.getLong();
			
			int length = buffer.limit() - 1 - ID_SIZE;

			final Object result;
			if (writerSchema.getType().equals(Schema.Type.BYTES)) {
				byte[] recordBytes = new byte[length];
				buffer.get(recordBytes, 0, length);
				result = recordBytes;
			} else {
				int start = buffer.position() + buffer.arrayOffset();
				DatumReader reader = getDatumReader(writerSchema, readerSchema);
				Object object = reader.read(null, decoderFactory.binaryDecoder(buffer.array(), start, length, null));

				if (writerSchema.getType().equals(Schema.Type.STRING)) {
					object = object.toString(); // Utf8 -> String
				}
				result = object;
			}
			return result;
		} catch (Exception e) {
			throw new SerializationException("Error deserializing Avro message for id " + schemaUniqId, e);
		}
	}

	protected boolean useSpecificAvroReader = false;

	@SuppressWarnings("rawtypes")
	private DatumReader getDatumReader(Schema writerSchema, Schema readerSchema) {
		boolean writerSchemaIsPrimitive = getPrimitiveSchemas().values().contains(writerSchema);
		// do not use SpecificDatumReader if writerSchema is a primitive
		if (useSpecificAvroReader && !writerSchemaIsPrimitive) {
			if (readerSchema == null) {
				readerSchema = getReaderSchema(writerSchema);
			}
			return new SpecificDatumReader(writerSchema, readerSchema);
		} else {
			if (readerSchema == null) {
				return new GenericDatumReader(writerSchema);
			}
			return new GenericDatumReader(writerSchema, readerSchema);
		}
	}

	private final Map<String, Schema> readerSchemaCache = new ConcurrentHashMap<String, Schema>();

	@SuppressWarnings("unchecked")
	private Schema getReaderSchema(Schema writerSchema) {
		Schema readerSchema = readerSchemaCache.get(writerSchema.getFullName());
		if (readerSchema == null) {
			Class<SpecificRecord> readerClass = SpecificData.get().getClass(writerSchema);
			if (readerClass != null) {
				try {
					readerSchema = readerClass.newInstance().getSchema();
				} catch (InstantiationException e) {
					throw new SerializationException(writerSchema.getFullName() + " specified by the "
							+ "writers schema could not be instantiated to " + "find the readers schema.");
				} catch (IllegalAccessException e) {
					throw new SerializationException(writerSchema.getFullName() + " specified by the "
							+ "writers schema is not allowed to be instantiated " + "to find the readers schema.");
				}
				readerSchemaCache.put(writerSchema.getFullName(), readerSchema);
			} else {
				throw new SerializationException("Could not find class " + writerSchema.getFullName()
						+ " specified in writer's schema whilst finding reader's " + "schema for a SpecificRecord.");
			}
		}
		return readerSchema;
	}
}

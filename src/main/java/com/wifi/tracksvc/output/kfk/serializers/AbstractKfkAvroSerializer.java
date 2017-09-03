package com.wifi.tracksvc.output.kfk.serializers;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;
import org.springframework.util.Assert;

import com.wifi.tracksvc.output.kfk.serializers.exception.SerializationException;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;

public class AbstractKfkAvroSerializer extends AbstractSRAwareKfkAvroSerDe {

	private final EncoderFactory encoderFactory = EncoderFactory.get();

	public AbstractKfkAvroSerializer(List<String> urls) {
		super(urls);
	}

	public AbstractKfkAvroSerializer(SchemaRegistryClient client) {
		super(client);
	}

	protected byte[] doSerialize(String topic, Object record, int schemaUniqId) {
		Assert.notNull(record, "record cant be null");
		
		Schema schema = null;
	    try {
	      schema = getRecordSchema(record);
	      ByteArrayOutputStream out = new ByteArrayOutputStream();
	      out.write(MAGIC_BYTE);
	      out.write(ByteBuffer.allocate(ID_SIZE).putInt(schemaUniqId).array());
	      out.write(ByteBuffer.allocate(TS_SIZE).putLong(System.currentTimeMillis()).array());
	      
	      if (record instanceof byte[]) {
	        out.write((byte[]) record);
	      } else {
	        BinaryEncoder encoder = encoderFactory.directBinaryEncoder(out, null);
	        DatumWriter<Object> writer;
	        Object value = record instanceof NonRecordContainer ? ((NonRecordContainer) record).getValue() : record;
	        if (value instanceof SpecificRecord) {
	          writer = new SpecificDatumWriter<>(schema);
	        } else {
	          writer = new GenericDatumWriter<>(schema);
	        }
	        writer.write(value, encoder);
	        encoder.flush();
	      }
	      byte[] bytes = out.toByteArray();
	      out.close();
	      return bytes;
	    } catch (Exception e) {
			throw new SerializationException("Error serializing Avro message", e);
	    } 
	}

}

package com.wifi.tracksvc.output.kfk.formatter;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.util.Utf8;

import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.pplkq.util.StringUtils;
import com.pplkq.util.compress.GzipUtils;
import com.wifi.tracksvc.output.kfk.serializers.AbstractKfkAvroSerializer;
import com.wifi.tracksvc.output.kfk.serializers.exception.SerializationException;
import com.wifi.tracksvc.request.FinderEvent;
import com.wifi.tracksvc.request.FinderRequest;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TrackSvcAvroMarshaller extends AbstractKfkAvroSerializer {

	public TrackSvcAvroMarshaller(List<String> urls) {
		super(urls);
	}

	public TrackSvcAvroMarshaller(SchemaRegistryClient client) {
		super(client);
	}
	
	public List<FinderEvent> marshall(final FinderRequest request){
		List<FinderEvent> ret = Lists.newArrayList();
		
		return ret;
	}
	
	//make it lru
	private final Map<String, Schema> recordSchemaCache = new ConcurrentHashMap<String, Schema>();
	private final DecoderFactory decoderFactory = DecoderFactory.get();
	private final Gson gson = new Gson();
	
	private List<FinderEvent> gzippedJsonArrayToAvro(byte[] payload) {
		return jsonArrayToAvro(GzipUtils.deCompress(payload));
	}
	
	private List<FinderEvent> jsonArrayToAvro(String jsonArray) {
		List<FinderEvent> evtList = Lists.newArrayList();
		
		String[] arr = gson.fromJson(jsonArray, String[].class);
		for(int i=0; i<arr.length; i++) {
			String item = arr[i];
			String subject = "";
			int versionId = 0;
			evtList.add((FinderEvent)jsonToAvro(item, getRecordSchemaFromJson(subject, versionId)));
		}
		return evtList;
	}
	
	private Schema getRecordSchemaFromJson(String subject, int versionId) {
		String key = StringUtils.join('-', subject, versionId);
		recordSchemaCache.putIfAbsent(key, this.getBySubjectAndId(subject, versionId));
		return recordSchemaCache.get(key);
	}

	private Object jsonToAvro(String jsonString, Schema schema) {
		try {
			DatumReader<Object> reader = new GenericDatumReader<Object>(schema);
			Object object = reader.read(null, decoderFactory.jsonDecoder(schema, jsonString));

			if (schema.getType().equals(Schema.Type.STRING)) {
				object = ((Utf8) object).toString();
			}
			return object;
		} catch (IOException e) {
			throw new SerializationException(
					String.format("Error deserializing json %s to Avro of schema %s", jsonString, schema), e);
		} catch (AvroRuntimeException e) {
			throw new SerializationException(
					String.format("Error deserializing json %s to Avro of schema %s", jsonString, schema), e);
		}
	}
}

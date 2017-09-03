package com.pplkq.io;

import static org.hamcrest.Matchers.is;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;

import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Test;

public class ByteBufferTest {

	protected static final byte MAGIC_BYTE = 0x0;
	protected static final int ID_SIZE = 4;
	protected static final int TS_SIZE = 8;

	@Test
	public void testByteBuffer() throws Exception {
		long ts = System.currentTimeMillis();
		String data = "i-am-data-part";
		int schemaUniqId = 123;
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		out.write(MAGIC_BYTE);
		out.write(ByteBuffer.allocate(ID_SIZE).putInt(schemaUniqId).array());
		out.write(ByteBuffer.allocate(TS_SIZE).putLong(ts).array());
		out.write(data.getBytes());
		out.flush();

		byte[] bytes = out.toByteArray();
		out.close();
		
		//read it
		ByteBuffer buffer = ByteBuffer.wrap(bytes);
		System.out.println("buffer length: " + buffer.array().length);
		
		Assert.assertThat(buffer.get(), is(MAGIC_BYTE));
		Assert.assertThat(buffer.getInt(), is(schemaUniqId));
		Assert.assertThat(buffer.getLong(), is(ts));
		
		int length = buffer.limit() - 1 - ID_SIZE - TS_SIZE;
		System.out.println("buffer length: " + buffer.array().length);
		System.out.println("data length: " + length);
		int start = buffer.position() + buffer.arrayOffset();
		System.out.println("start: " + start);
		
		byte[] b = new byte[length];
		int i=0;
		for(; i<length; i++) {
			b[i] = buffer.array()[i+start];
		}
		System.out.println("i: " + i);
		System.out.println("data: " + new String(b));
	}
}

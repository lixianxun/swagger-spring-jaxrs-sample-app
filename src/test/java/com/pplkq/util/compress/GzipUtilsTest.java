package com.pplkq.util.compress;

import static org.hamcrest.CoreMatchers.is;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import org.junit.Assert;
import org.junit.Test;

import com.google.common.base.Stopwatch;

public class GzipUtilsTest {

	@Test
	public void testGzipCompressDecompress() throws Exception{
		String plainText = "plain text used to test gzip compress/decompress";
		ByteArrayInputStream input = new ByteArrayInputStream(plainText.getBytes(StandardCharsets.UTF_8));
		
		ByteArrayOutputStream gzippedStream = new ByteArrayOutputStream();
		GzipUtils.compress(input, gzippedStream);
		
		ByteArrayOutputStream output = new ByteArrayOutputStream();
		GzipUtils.deCompress(new ByteArrayInputStream(gzippedStream.toByteArray()), output);
		
		Assert.assertThat(new String(output.toByteArray()), is(plainText));
		Assert.assertThat(GzipUtils.deCompress(gzippedStream.toByteArray()), is(plainText));
	}
	
	@Test
	public void testGzipCompressDecompress2() throws Exception{
		String plainText = "plain text used to test gzip compress/decompress";
		ByteArrayInputStream input = new ByteArrayInputStream(plainText.getBytes(StandardCharsets.UTF_8));
		
		ByteArrayOutputStream gzippedStream = new ByteArrayOutputStream();
		GzipUtils.compress(input, gzippedStream);
		
		Assert.assertThat(GzipUtils.deCompress(gzippedStream.toByteArray()), is(plainText));
	}
	
	@Test
	public void testGzipCompressDecompressPerf() throws Exception{
		int count = 20 * 10000;
		Stopwatch timer = Stopwatch.createStarted();
		
		IntStream.rangeClosed(1, count)
        	.forEach( i -> {try {
				testGzipCompressDecompress();
			} catch (Exception e) {
				e.printStackTrace();
			}});
		System.out.println(timer.stop().elapsed(TimeUnit.SECONDS));
	}
	
	@Test
	public void testGzipDecompressPerf() throws Exception{
		int count = 40 * 1000;
		
		String plainText = "plain text used to test gzip compress/decompress";
		ByteArrayInputStream input = new ByteArrayInputStream(plainText.getBytes(StandardCharsets.UTF_8));
		ByteArrayOutputStream gzippedStream = new ByteArrayOutputStream();
		GzipUtils.compress(input, gzippedStream);
		
		Stopwatch timer = Stopwatch.createStarted();
		IntStream.rangeClosed(1, count)
			.forEach( i -> {try {
				ByteArrayOutputStream output = new ByteArrayOutputStream();
				GzipUtils.deCompress(new ByteArrayInputStream(gzippedStream.toByteArray()), output);
				Assert.assertThat(new String(output.toByteArray()), is(plainText));
			} catch (Exception e) {
				e.printStackTrace();
			}});
		System.out.println(timer.stop().elapsed(TimeUnit.MILLISECONDS)/1000f);
	}
}

package com.pplkq.util.compress;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.Charset;

import org.apache.commons.compress.compressors.CompressorInputStream;
import org.apache.commons.compress.compressors.CompressorOutputStream;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.apache.commons.compress.utils.IOUtils;

public class GzipUtils {

	public static void compress(final InputStream input, final OutputStream gzippedStream) {
		try {
			CompressorOutputStream gzippedOut = new CompressorStreamFactory()
					.createCompressorOutputStream(CompressorStreamFactory.GZIP, gzippedStream);
			
			IOUtils.copy(input, gzippedOut);
			gzippedOut.flush();
			gzippedOut.close();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}finally {
			IOUtils.closeQuietly(input);
			IOUtils.closeQuietly(gzippedStream);
		}
	}
	
	public static byte[] compress(final String data) {
		ByteArrayOutputStream output = new ByteArrayOutputStream();
		try {
			CompressorOutputStream gzippedOut = new CompressorStreamFactory()
					.createCompressorOutputStream(CompressorStreamFactory.GZIP, output);
			
			IOUtils.copy(new ByteArrayInputStream(data.getBytes()), gzippedOut);
			gzippedOut.flush();
			gzippedOut.close();
			
			return output.toByteArray();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}finally {
			if(output!=null)IOUtils.closeQuietly(output);
		}
	}
	
	public static void deCompress(final InputStream gzippedInputStream, final OutputStream output) {
		try {
			CompressorInputStream gzippedIn = new CompressorStreamFactory()
					.createCompressorInputStream(CompressorStreamFactory.GZIP, gzippedInputStream);
			
			IOUtils.copy(gzippedIn, output);
			gzippedIn.close();
			output.flush();
			output.close();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}finally {
			IOUtils.closeQuietly(gzippedInputStream);
			IOUtils.closeQuietly(output);
		}
	}
	
	public static String deCompress(final byte[] gzippedInputBytes) {
		CompressorInputStream gzippedIn = null;
		ByteArrayOutputStream output = null;
		try {
			gzippedIn = new CompressorStreamFactory()
					.createCompressorInputStream(CompressorStreamFactory.GZIP, new ByteArrayInputStream(gzippedInputBytes));
			output = new ByteArrayOutputStream();
			
			IOUtils.copy(gzippedIn, output);
			gzippedIn.close();
			output.flush();
			output.close();
			return new String(output.toByteArray(), Charset.defaultCharset());
		} catch (Exception e) {
			throw new RuntimeException(e);
		}finally {
			if(gzippedIn!=null)IOUtils.closeQuietly(gzippedIn);
			if(output!=null)IOUtils.closeQuietly(output);
		}
	}
	
}

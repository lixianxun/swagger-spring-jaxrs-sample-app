package com.pplkq.util.compress;

import java.io.InputStream;
import java.io.OutputStream;

import org.apache.commons.compress.compressors.CompressorInputStream;
import org.apache.commons.compress.compressors.CompressorOutputStream;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.apache.tomcat.util.http.fileupload.IOUtils;

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
	
}

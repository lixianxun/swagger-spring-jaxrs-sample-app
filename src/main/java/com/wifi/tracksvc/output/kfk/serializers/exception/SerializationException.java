package com.wifi.tracksvc.output.kfk.serializers.exception;

public class SerializationException extends RuntimeException {

	private static final long serialVersionUID = 1L;

	public SerializationException(Exception e) {
		super(e);
	}

	public SerializationException(String msg, Exception e) {
		super(msg, e);
	}

	public SerializationException(String msg) {
		super(msg);
	}

}

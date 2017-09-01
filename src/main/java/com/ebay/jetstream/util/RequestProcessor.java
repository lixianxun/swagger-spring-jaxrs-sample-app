package com.ebay.jetstream.util;

import java.util.List;

public interface RequestProcessor {

	public boolean processRequest(Runnable req);
	public boolean processBatch(List<Runnable> requests);
}

package com.pplkq.resources;

import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Test;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;

import com.google.common.base.Stopwatch;

import jersey.repackaged.com.google.common.collect.Maps;

public class TestRestResource {

	static {
		System.setProperty("org.apache.commons.logging.Log", "org.apache.commons.logging.impl.NoOpLog");
	}

	String url = "http://localhost:8080/app/rest/users";
	RestTemplate tpl = new RestTemplate();

	Map<String, AtomicLong> status = Maps.newConcurrentMap();

	@Test
	public void testRestResourse() throws Exception {
		int nThreads = 4;
		int countPerThread = 100000;
		AtomicLong total = new AtomicLong();

		Stopwatch timer = Stopwatch.createStarted();
		ExecutorService executor = Executors.newFixedThreadPool(nThreads);
		
		for(int i=0; i<nThreads; i++) {
			executor.submit(() -> {
				for (int j = 0; j < countPerThread; j++) {
					ResponseEntity<String> resp = tpl.exchange(url, HttpMethod.GET, null, String.class);
					count(resp.getStatusCode().toString());
					total.incrementAndGet();
				}
			});
		}

		shutdownExecutor(executor);
		float f = timer.stop().elapsed(TimeUnit.MILLISECONDS) / 1000f;

		Thread.sleep(1000);
		System.out.println(status);
		System.out.println("total reqeust: " + total);
		System.out.println("time elapsed: " + f);
		System.out.println("throughput: " + total.get() / f);
	}

	private void shutdownExecutor(ExecutorService executor) {
		try {
			System.out.println("attempt to shutdown executor");
			executor.shutdown();
			executor.awaitTermination(5, TimeUnit.MINUTES);
		} catch (InterruptedException e) {
			System.err.println("tasks interrupted");
		} finally {
			if (!executor.isTerminated()) {
				System.err.println("cancel non-finished tasks");
			}
			executor.shutdownNow();
			System.out.println("shutdown finished");
		}
	}

	private void count(String key) {
		status.putIfAbsent(key, new AtomicLong(0));
		status.get(key).incrementAndGet();
	}
}

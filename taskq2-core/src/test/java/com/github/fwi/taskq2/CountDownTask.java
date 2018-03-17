package com.github.fwi.taskq2;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

public class CountDownTask {
	
	public static AtomicInteger ID = new AtomicInteger();

	public CountDownLatch running = new CountDownLatch(1);
	public CountDownLatch finish  = new CountDownLatch(1);
	public CountDownLatch done  = new CountDownLatch(1);
	public int id = ID.incrementAndGet();
	public String qosKey;
	
	@Override
	public String toString() {
		return this.getClass().getSimpleName() + " " + id;
	}
}
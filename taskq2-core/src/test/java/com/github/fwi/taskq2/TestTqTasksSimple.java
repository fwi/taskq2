package com.github.fwi.taskq2;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.*;

public class TestTqTasksSimple {

	private static TqBase tq;
	private static TqGroup tgroup;

	@BeforeClass
	public static void setupTq() {

		tgroup = new TqGroup();
		tq = new TqFifo();
		tgroup.addQueue(tq);
		tgroup.start();
	}
	
	@AfterClass
	public static void stopTq() {

		if (tgroup != null) {
			tgroup.stop();
		}
	}

	@Test
	public void runOneTask() {
		
		// this is not thread-safe (handler should not be updated), but OK for test.
		tq.setTasksHandler(new TaskHandler() {
			@Override
			public void onTask(Object tdata) {
				((CountDownLatch) tdata).countDown();
			}
		});
		try {
			CountDownLatch latch;
			tgroup.addTask(tq.getName(), latch = new CountDownLatch(1));
			if (!latch.await(1L, TimeUnit.SECONDS)) {
				fail("Expected task to be executed.");
			}
		} catch (Exception e) {
			e.printStackTrace();
			fail();
		}
	}

	@Test
	public void loopOneTask() {
		
		final int maxLoops = 10;
		tq.setTasksHandler(new TaskHandler() {
			
			final AtomicInteger counter = new AtomicInteger();
			@Override
			public void onTask(Object tdata) {
				if (counter.incrementAndGet() > maxLoops) {
					((CountDownLatch) tdata).countDown();
				} else {
					tgroup.addTask(tq.getName(), tdata);
				}
			}
		});
		try {
			CountDownLatch latch;
			tgroup.addTask(tq.getName(), latch = new CountDownLatch(1));
			if (!latch.await(1L, TimeUnit.SECONDS)) {
				fail("Expected looping tasks to be executed.");
			}
			assertTrue(tgroup.getTasksExecuted() >= maxLoops);
		} catch (Exception e) {
			e.printStackTrace();
			fail();
		}
	}

}

package com.github.fwi.taskq2;

import static org.junit.Assert.*;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestTqTasksCountDown {

	@SuppressWarnings("unused")
	private static final Logger log = LoggerFactory.getLogger(TestTqTasksCountDown.class);

	private static TqBase tq;
	private static TqGroup tgroup;

	@BeforeClass
	public static void setupTq() {

		tq = new TqFifo();
		tq.setTasksHandler(new CountDownTaskHandler());
		tgroup = new CountDownTqGroup();
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
	public void oneTask() {

		// Check execution of one task
		CountDownTask task = new CountDownTask();
		tgroup.addTask(tq.getName(), task);
		try {
			assertTrue("Executing first task.", task.running.await(1, TimeUnit.SECONDS));
			task.finish.countDown();
			assertTrue("Removing first task.", task.done.await(1, TimeUnit.SECONDS));
		} catch (Exception e) {
			e.printStackTrace();
			fail();
		}
	}

	@Test
	public void maxConcurrentPlusOneTask() {

		// Check max amount tasks concurrent, add MaxInConcurrent + 1 tasks to queue.
		CountDownLatch taskRun = new CountDownLatch(tq.getMaxConcurrent());
		CountDownLatch taskFinish = new CountDownLatch(1);
		CountDownLatch taskDone = new CountDownLatch(tq.getMaxConcurrent());
		for (int i = 0; i < tq.getMaxConcurrent(); i++) {
			CountDownTask task = new CountDownTask();
			task.running = taskRun;
			task.finish = taskFinish;
			task.done = taskDone;
			tgroup.addTask(tq.getName(), task);
		}
		// When max amount tasks finished, next task should be picked up.
		CountDownTask lastTask = new CountDownTask();
		tgroup.addTask(tq.getName(), lastTask);
		try {
			assertTrue("Executing tasks.", taskRun.await(1, TimeUnit.SECONDS));
			assertEquals("No more than max tasks running at the same time.", tq.getMaxConcurrent(), tq.getInProgress());
			taskFinish.countDown();
			assertTrue("Removing tasks.", taskDone.await(1, TimeUnit.SECONDS));
			assertTrue("Executing last task.", lastTask.running.await(1, TimeUnit.SECONDS));
			assertEquals("Previous tasks are finished", 1, tq.getInProgress());
			lastTask.finish.countDown();
			assertTrue("Last task removed.", lastTask.done.await(1, TimeUnit.SECONDS));
		} catch (Exception e) {
			e.printStackTrace();
			fail();
		}
	}

}

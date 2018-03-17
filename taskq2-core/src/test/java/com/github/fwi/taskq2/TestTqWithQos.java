package com.github.fwi.taskq2;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("unused")
public class TestTqWithQos {

	private final Logger log = LoggerFactory.getLogger(this.getClass());

	@Test
	public void testTaskQos() throws Exception {
		
		// Check execution of one task with null Qos-key
		TqQos tq = new TqQos();
		tq.setTasksHandler(new CountDownTaskHandler());
		tq.setMaxConcurrent(1);
		TqGroup tgroup = new CountDownTqGroup();
		tgroup.addQueue(tq);
		tgroup.start();
		CountDownTask task = new CountDownTask();
		tgroup.addTask(tq.getName(), task);
		boolean testOk = false;
		try {
			assertTrue("Executing first task.", task.running.await(1, TimeUnit.SECONDS));
			assertEquals("One qos task in progress.", 1, tq.getInProgress());
			task.finish.countDown();
			assertTrue("Removed first task.", task.done.await(1, TimeUnit.SECONDS));
			assertEmpty(tgroup, tq);
			testOk = true;
		} finally {
			if (!testOk) {
				tgroup.stop();
			}
		}
		
		// Check alternating between Qos keys.
		tgroup.setPaused(tq.getName(), true);
		List<CountDownTask> tlist = new ArrayList<>();
		for (int i = 0; i < 10; i++) {
			task = new CountDownTask();
			task.id = i;
			if (i < 3) {
				task.qosKey = null;
			} else if (i < 6) {
				task.qosKey = "one";
			} else {
				task.qosKey = "two";
			}
			tlist.add(task);
			tgroup.addTask(tq.getName(), task, task.qosKey);
		}
		// Expected order of task execution.
		List<Integer> torder = new ArrayList<>();
		torder.add(0);
		torder.add(3);
		torder.add(6);
		torder.add(1);
		torder.add(4);
		torder.add(7);
		torder.add(2);
		torder.add(5);
		torder.add(8);
		torder.add(9);
		
		tq.setMaxConcurrentPerQosKey(1);
		tgroup.setPaused(tq.getName(), false);
		testOk = false;
		try {
			for (int i = 0; i < torder.size(); i++) {
				int taskNumber = torder.get(i);
				task = tlist.get(taskNumber);
				// System.out.println("Waiting for " + i + " / " + taskNumber + " / " + task.id + ", remaining: " + tgroup.getSize());
				assertTrue("Executing qos task " + taskNumber, task.running.await(1, TimeUnit.SECONDS));
				task.finish.countDown();
			}
			assertTrue("All tasks finished.", tgroup.awaitAllTasksDone(1, TimeUnit.SECONDS));
			assertEmpty(tgroup, tq);
			testOk = true;
		} finally {
			if (!testOk) {
				tgroup.stop();
			}
		}
		tgroup.stop();
	}

	private void assertEmpty(TqGroup tgroup, TqQos tq) {
	
		assertEquals("No qos tasks remaining.", 0, tgroup.getSize());
		assertEquals("No qos keys used.", 0, tq.getSizeKeys());
		assertEquals("No qos task in progress.", 0, tq.getInProgress());
	}
}

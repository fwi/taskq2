package com.github.fwi.taskq2;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CountDownTaskHandler extends TaskHandler {

	private static final Logger log = LoggerFactory.getLogger(CountDownTaskHandler.class);

	@Override
	public void onTask(Object tdata) {

		CountDownTask t = (CountDownTask) tdata;
		try {
			t.running.countDown();
			log.debug("Task " + t.id + " running.");
			t.finish.await();
			log.debug("Task " + t.id + " finished.");
		} catch (Exception e) {
			throw new AssertionError("Task " + t.id  + "  was not executed.", e);
		}
	}

}

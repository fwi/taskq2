package com.github.fwi.taskq2;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.fwi.taskq2.TqBase;
import com.github.fwi.taskq2.TqEntry;
import com.github.fwi.taskq2.TqGroup;

public class CountDownTqGroup extends TqGroup {

	private static final Logger log = LoggerFactory.getLogger(CountDownTqGroup.class);

	@Override 
	protected void taskDone(TqBase tq, TqEntry te) {
	
		super.taskDone(tq, te);
		CountDownTask t = (CountDownTask) te.getTaskData();
		t.done.countDown();
		log.debug("Task " + t.id + " done.");
	}

}

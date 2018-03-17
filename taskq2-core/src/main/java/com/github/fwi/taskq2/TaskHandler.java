package com.github.fwi.taskq2;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.fwi.taskq2.util.PrettyPrintMap;

public class TaskHandler implements ITaskHandler {

	private static final Logger log = LoggerFactory.getLogger(TaskHandler.class);
	
	@Override
	public void onTask(Object tdata, String qname, String qosKey, long taskId) {
		onTask(tdata);
	}
	
	public void onTask(Object tdata) {
		
		if (log.isDebugEnabled()) {
			log.debug("Task data: {}", (tdata instanceof Map<?, ?> ? new PrettyPrintMap((Map<?,?>) tdata) : tdata));
		}
	}

}

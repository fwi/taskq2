package com.github.fwi.taskq2.db.poll;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.fwi.taskq2.ITaskHandlerFactory;
import com.github.fwi.taskq2.TqEntry;
import com.github.fwi.taskq2.TqFifo;
import com.github.fwi.taskq2.db.DbTestTask;

public class TqDbTest extends TqFifo {
	
	private static final Logger log = LoggerFactory.getLogger(TqDbTest.class);

	public final CountDownLatch haveQueued = new CountDownLatch(1);
	public volatile DbTestTask lastQueued;
	
	public TqDbTest() {
		this(null, null);
	}

	public TqDbTest(String name, ITaskHandlerFactory handlerFactory) {
		super(name, handlerFactory);
	}

	@Override
	public void addTask(TqEntry te) {
		
		lastQueued = (DbTestTask) te.getTaskData();
		log.trace("[{}] Adding task data [{}]", getName(), te.getTaskData().toString());
		haveQueued.countDown();
		super.addTask(te);
	}
	
	public DbTestTask getLastQueued() throws InterruptedException {
		
		if (!haveQueued.await(1L, TimeUnit.SECONDS)) {
			throw new RuntimeException("No task queued within 1 second."); 
		}
		return lastQueued;
	}

}

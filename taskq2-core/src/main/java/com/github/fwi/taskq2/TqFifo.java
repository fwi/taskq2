package com.github.fwi.taskq2;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class TqFifo extends TqBase {

	private final ConcurrentLinkedQueue<TqEntry> queue = new ConcurrentLinkedQueue<>();
	private final AtomicInteger qsize = new AtomicInteger();

	public TqFifo() {
		this(null, null);
	}

	public TqFifo(String name, ITaskHandlerFactory handlerFactory) {
		super(name, handlerFactory);
	}

	@Override
	public void addTask(TqEntry te) {
		
		queue.add(te);
		qsize.incrementAndGet();
	}
	
	@Override
	public TqEntry getNextTask() {
		
		TqEntry tqdata = queue.poll();
		if (tqdata != null) {
			qsize.decrementAndGet();
			inProgress.incrementAndGet();
		}
		return tqdata;
	}

	@Override
	public int getSize() {
		return qsize.get();
	}

}

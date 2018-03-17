package com.github.fwi.taskq2;

import java.util.concurrent.atomic.AtomicInteger;

public abstract class TqBase {

	private static final AtomicInteger qnumber = new AtomicInteger();
	
	private String name = this.getClass().getSimpleName() + "-" + qnumber.incrementAndGet();
	private ITaskHandlerFactory handlerFactory; 
	protected final AtomicInteger inProgress = new AtomicInteger();
	
	private volatile int maxConcurrent = 4;
	private volatile boolean paused;
	
	public TqBase() {
		this(null, null);
	}

	public TqBase(String name, ITaskHandlerFactory handlerFactory) {
		super();
		if (name != null) {
			this.name = name;
		}
		this.handlerFactory = handlerFactory;
	}

	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	
	public void setTasksHandler(ITaskHandler tasksHandler) {
		setHandlerFactory(new SingletonTaskHandlerFactory(tasksHandler));
	}
	
	public ITaskHandlerFactory getHandlerFactory() {
		return handlerFactory;
	}
	public void setHandlerFactory(ITaskHandlerFactory handlerFactory) {
		this.handlerFactory = handlerFactory;
	}

	public void addTask(Object tdata) {
		addTask(new TqEntry(tdata));
	}
	public abstract void addTask(TqEntry te);

	public abstract TqEntry getNextTask();

	public int getMaxConcurrent() {
		return maxConcurrent;
	}
	public void setMaxConcurrent(int maxConcurrent) {
		this.maxConcurrent = maxConcurrent;
	}
	
	public boolean isPaused() {
		return paused;
	}
	public void setPaused(boolean paused) {
		this.paused = paused;
	}
	
	public abstract int getSize();
	
	public int getInProgress() {
		return inProgress.get();
	}

	public void taskDone(TqEntry te) {
		inProgress.decrementAndGet();
	}

}

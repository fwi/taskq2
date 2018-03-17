package com.github.fwi.taskq2;

public class SingletonTaskHandlerFactory implements ITaskHandlerFactory {

	private final ITaskHandler tasksHandler;
	
	public SingletonTaskHandlerFactory(ITaskHandler tasksHandler) {
		this.tasksHandler = tasksHandler;
	}
	
	@Override
	public ITaskHandler getTaskHandler(String qname) {
		return tasksHandler;
	}

}

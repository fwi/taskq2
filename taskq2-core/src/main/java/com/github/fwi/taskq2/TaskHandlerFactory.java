package com.github.fwi.taskq2;

public class TaskHandlerFactory implements ITaskHandlerFactory {

	@Override
	public ITaskHandler getTaskHandler(String qname) {
		return new TaskHandler();
	}

}

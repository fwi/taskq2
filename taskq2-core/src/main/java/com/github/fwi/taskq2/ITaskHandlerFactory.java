package com.github.fwi.taskq2;

public interface ITaskHandlerFactory {

	ITaskHandler getTaskHandler(String qname);
}

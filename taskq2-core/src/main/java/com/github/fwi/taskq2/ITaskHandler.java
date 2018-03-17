package com.github.fwi.taskq2;

public interface ITaskHandler {

	void onTask(Object tdata, String qname, String qosKey, long taskId);

}

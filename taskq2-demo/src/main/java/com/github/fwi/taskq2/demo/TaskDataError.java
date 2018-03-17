package com.github.fwi.taskq2.demo;

import java.io.Serializable;

/**
 * The expected task-data for TQ_ERROR.
 */
public class TaskDataError implements Serializable {

	private static final long serialVersionUID = 5206890483980851537L;

	public long errorTaskId;
	public Exception e;

	TaskDataError(long errorTaskId, Exception e) {
		this.errorTaskId = errorTaskId;
		this.e = e;
	}

}

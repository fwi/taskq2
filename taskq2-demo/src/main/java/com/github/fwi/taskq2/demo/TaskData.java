package com.github.fwi.taskq2.demo;

import java.io.Serializable;

/**
 * The task-data used by the queues.
 * For more complex data, take note of the details in <tt>taskq2-jdbc/src/test/java com.github.fwi.taskq2.db.DbTestTask</tt>
 */
public class TaskData implements Serializable {
	
	private static final long serialVersionUID = 2177242352969855126L;

	public byte[] bytes;
	public String b64;
	public String md5;

	TaskData(byte[] bytes) {
		this.bytes = bytes;
	}
}
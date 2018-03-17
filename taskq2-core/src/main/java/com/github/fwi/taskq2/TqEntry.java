package com.github.fwi.taskq2;

public class TqEntry {

	private Object taskData;
	private String qosKey;
	private long taskId;

	public TqEntry() { super(); }
	
	public TqEntry(Object tdata) {
		this(tdata, null);
	}
	
	public TqEntry(Object tdata, String qosKey) {
		this(tdata, qosKey, 0L);
	}

	public TqEntry(Object taskData, String qosKey, long taskId) {
		this.taskData = taskData;
		this.qosKey = qosKey;
		this.taskId = taskId;
	}

	public Object getTaskData() {
		return taskData;
	}

	public void setTaskData(Object taskData) {
		this.taskData = taskData;
	}

	public String getQosKey() {
		return qosKey;
	}

	public void setQosKey(String qosKey) {
		this.qosKey = qosKey;
	}

	public long getTaskId() {
		return taskId;
	}

	public void setTaskId(long taskId) {
		this.taskId = taskId;
	}

}

package com.github.fwi.taskq2.db;

public class TqDbEntry {

	private String qname;
	private String qosKey;
	private long taskId;
	private int serverId;
	private byte[] taskDataBytes;
	private long expireDate;
	private int retryCount;
	
	public TqDbEntry() { super(); }

	public TqDbEntry(String qname) {
		this(qname, null);
	}

	public TqDbEntry(String qname, String qosKey) {
		this(qname, qosKey, 0L);
	}
	
	public TqDbEntry(String qname, String qosKey, long taskId) {
		this.qname = qname;
		this.qosKey = qosKey;
		this.taskId = taskId;
	}

	public String getQname() {
		return qname;
	}
	public void setQname(String qname) {
		this.qname = qname;
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
	public int getServerId() {
		return serverId;
	}
	public void setServerId(int serverId) {
		this.serverId = serverId;
	}
	public long getExpireDate() {
		return expireDate;
	}
	public void setExpireDate(long expireDate) {
		this.expireDate = expireDate;
	}
	public byte[] getTaskDataBytes() {
		return taskDataBytes;
	}
	public void setTaskDataBytes(byte[] taskData) {
		this.taskDataBytes = taskData;
	}
	public int getRetryCount() {
		return retryCount;
	}
	public void setRetryCount(int retryCount) {
		this.retryCount = retryCount;
	}
	
}

package com.github.fwi.taskq2.db;

public class TqQueryNames {

	private TqQueryNames() {}
	
	public static final String MERGE_PROP = "TASKQ.MERGE_PROP";
	public static final String GET_PROP = "TASKQ.GET_PROP";
	public static final String DELETE_PROP = "TASKQ.DELETE_PROP";
	
	public static final String FIND_SERVER = "TASKQ.FIND_SERVER";
	public static final String INSERT_SERVER = "TASKQ.INSERT_SERVER";
	public static final String SERVER_ACTIVE = "TASKQ.SERVER_ACTIVE";
	
	public static final String TASKS_ACTIVE = "TASKQ.TASKS_ACTIVE";
	public static final String INSERT_TASK = "TASKQ.INSERT_TASK";
	public static final String INSERT_ITEM = "TASKQ.INSERT_ITEM";
	public static final String LOAD_TASK = "TASKQ.LOAD_TASK";
	public static final String DELETE_TASK = "TASKQ.DELETE_TASK";
	public static final String ABANDON_TASK = "TASKQ.ABANDON_TASK";
	public static final String UPDATE_TASK_QNAME = "TASKQ.UPDATE_TASK_QNAME";
	public static final String UPDATE_TASK_RETRY = "TASKQ.UPDATE_TASK_RETRY";
	
	public static final String HAVE_EXPIRED = "TASKQ.HAVE_EXPIRED";
	public static final String EXPIRED_PER_Q= "TASKQ.EXPIRED_PER_Q";
	public static final String UPDATE_EXPIRED = "TASKQ.UPDATE_EXPIRED";
	
	public static final String DEAD_SERVERS = "TASKQ.DEAD_SERVERS";
	public static final String LOCK_SERVER = "TASKQ.LOCK_SERVER";
	public static final String UPDATE_FAIL_OVER = "TASKQ.UPDATE_FAIL_OVER";
	public static final String ABANDON_DEAD_SERVER = "TASKQ.ABANDON_DEAD_SERVER";
}

package com.github.fwi.taskq2.db;

import java.nio.charset.StandardCharsets;

import nl.fw.util.jdbc.INamedQuery;

public class TqDbConf {

	/* *** DB Init *** */
	
	private String dbResourceCharsetName = StandardCharsets.UTF_8.name(); 
	private String namedQueriesResource = "taskq-db-queries.sql";
	private String dbStructResource = "taskq-db-struct-hsqldb.sql";
	private String dbInitQueriesResource = "taskq-db-data-init.sql";
	private INamedQuery namedQueries;
			
	public String getDbResourceCharsetName() {
		return dbResourceCharsetName;
	}
	public void setDbResourceCharsetName(String dbResourceCharsetName) {
		this.dbResourceCharsetName = dbResourceCharsetName;
	}
	public String getNamedQueriesResource() {
		return namedQueriesResource;
	}
	public void setNamedQueriesResource(String namedQueriesResource) {
		this.namedQueriesResource = namedQueriesResource;
	}
	public String getDbStructResource() {
		return dbStructResource;
	}
	public void setDbStructResource(String dbStructResource) {
		this.dbStructResource = dbStructResource;
	}
	public String getDbInitQueriesResource() {
		return dbInitQueriesResource;
	}
	public void setDbInitQueriesResource(String dbInitQueriesResource) {
		this.dbInitQueriesResource = dbInitQueriesResource;
	}
	public INamedQuery getNamedQueries() {
		return namedQueries;
	}
	public void setNamedQueries(INamedQuery namedQueries) {
		this.namedQueries = namedQueries;
	}

	/* *** DB server *** */
	
	private String serverHost = "localhost";
	private int serverPort = 9321;
	private String serverGroup = "default";
	private boolean useHostNameAsServerName = true;
	
	public String getServerHost() {
		return serverHost;
	}
	public void setServerHost(String serverHost) {
		this.serverHost = serverHost;
	}
	public int getServerPort() {
		return serverPort;
	}
	public void setServerPort(int serverPort) {
		this.serverPort = serverPort;
	}
	public String getServerGroup() {
		return serverGroup;
	}
	public void setServerGroup(String serverGroup) {
		this.serverGroup = serverGroup;
	}
	public boolean isUseHostNameAsServerName() {
		return useHostNameAsServerName;
	}
	public void setUseHostNameAsServerName(boolean useHostNameAsServerName) {
		this.useHostNameAsServerName = useHostNameAsServerName;
	}
	
	/* *** DB group *** */
	
	private int expireTimeS = 120;
	private int failOverTimeoutS = 30;
	private long heartBeatIntervalMs = 3_000L;
	private boolean noHeartBeatPause;
	private long reloadIntervalMs = 5_000L;
	private int maxSize;
	private int maxSizePerQ;
	private int dbReloadMinFree = 20;
	private int dbReloadMaxAmount = 0;
	private int dbReloadLogAmount = 0;
	private boolean noFailOver;
	private int dbGracePeriodS = 60;

	/** 
	 * Period in seconds after which a work task will be reloaded from the database 
	 * if it is not updated in the mean time (default 2 minutes). 
	 */
	public int getExpireTimeS() {
		return expireTimeS;
	}
	/** See {@link #getExpireTimeS()} */
	public void setExpireTimeS(int expireTimeS) {
		this.expireTimeS = expireTimeS;
	}
	
	/** 
	 * Default inactive period at which this server considers other servers in the same group dead
	 * (i.e. 'last_active' date was not updated within this inactive period).
	 * If this server finds that another server is dead, this server will attempt to take over (fail over) the task records
	 * from the other server and sets 'abandoned' to true for the dead server. 
	 * <br>Default 30 seconds. 
	 * <br>Half this value is used as default fail-over poll interval. */
	public int getFailOverTimeoutS() {
		return failOverTimeoutS;
	}
	/** See {@link #getFailOverTimeoutS()} */
	public void setFailOverTimeoutS(int failOverTimeoutS) {
		this.failOverTimeoutS = failOverTimeoutS;
	}

	/** 
	 * Default interval at which server record "last active" is updated.
	 * <br>Default 3 seconds. 
	 */
	public long getHeartBeatIntervalMs() {
		return heartBeatIntervalMs;
	}
	/** See {@link #getHeartBeatIntervalMs()} */
	public void setHeartBeatIntervalMs(long heartBeatIntervalMs) {
		this.heartBeatIntervalMs = heartBeatIntervalMs;
	}

	/**
	 * By default the heart beat poller will pause task execution when the database is unavailable.
	 * If this method returns true, task execution is not suspended by the heart beat poller. 
	 */
	public boolean isNoHeartBeatPause() {
		return noHeartBeatPause;
	}
	/** See {@link #isNoHeartBeatPause()} */
	public void setNoHeartBeatPause(boolean noHeartBeatPause) {
		this.noHeartBeatPause = noHeartBeatPause;
	}
	/** 
	 * Default interval at wich server checks for task records that have expired date before now
	 * which indicates they should be reloaded.
	 * <br>Default 5 seconds. 
	 */
	public long getReloadIntervalMs() {
		return reloadIntervalMs;
	}
	/** See {@link #getReloadIntervalMs()} */
	public void setReloadIntervalMs(long reloadIntervalMs) {
		this.reloadIntervalMs = reloadIntervalMs;
	}
	/**
	 * Maximum amount of tasks in all queues.
	 * @return 0 for unlimited size.
	 */
	public int getMaxSize() {
		return maxSize;
	}
	/** See {@link #getMaxSize()} */
	public void setMaxSize(int maxSize) {
		this.maxSize = maxSize;
	}

	/**
	 * Maximum amount of tasks in one queue.
	 * @return 0 for no size set, check {@link #getMaxSize()} instead.
	 */
	public int getMaxSizePerQ() {
		return maxSizePerQ;
	}
	/**
	 * Maximum amount of tasks in one queue.
	 * @param maxSizePerQ use 0 to indicate to use {@link #getMaxSize()} instead, else a positive number.
	 */
	public void setMaxSizePerQ(int maxSizePerQ) {
		this.maxSizePerQ = maxSizePerQ;
	}

	/**
	 * The minimum percentage of max/queue-size to keep free when reloading expired tasks
	 * (default 20). A higher percentage prevents in-memory tasks 
	 * from being pushed out by tasks reloaded from the database.  
	 */
	public int getDbReloadMinFree() { return dbReloadMinFree; }
	
	private int within100(int percentage) {
		
		if (percentage < 1) {
			return 0;
		} 
		if (percentage > 100) {
			return 100;
		} 
		return percentage;
	}
	
	/** See {@link #getDbReloadMinFree()} */
	public void setDbReloadMinFree(int dbReloadMinFree) {
		this.dbReloadMinFree = within100(dbReloadMinFree);
	}

	/**
	 * The maximum amount of records to reload in one reload-session for one queue 
	 * (default 0 for unlimited).
	 * <br>Setting this number to about {@link #getMaxSizePerQ()} 
	 * will prevent one queue from delaying reloads of other queues.
	 */
	public int getDbReloadMaxAmount() { return dbReloadMaxAmount; }

	/** See {@link #getDbReloadMaxAmount()}. */
	public void setDbReloadMaxAmount(int dbReloadMaxAmount) {
		this.dbReloadMaxAmount = dbReloadMaxAmount;
	}

	/**
	 * Emits a debug log-statement whenever the set amount of tasks 
	 * are reloaded and/or updated for one queue (default 0 for disabled). 
	 */
	public int getDbReloadLogAmount() { return dbReloadLogAmount; }

	/** See {@link #getDbReloadLogAmount()}. */
	public void setDbReloadLogInterval(int dbReloadLogAmount) {
		this.dbReloadLogAmount = dbReloadLogAmount;
	}

	/**
	 * Default false. If true, the fail-over poller is not started 
	 * and this server will never take-over task-records from another TaskQ server.
	 */
	public boolean isNoFailOver() {
		return noFailOver;
	}
	/** See {@link #isNoFailOver()} */
	public void setNoFailOver(boolean noFailOver) {
		this.noFailOver = noFailOver;
	}
	
	/** 
	 * If database was unavailable, how long to wait before checking for fail-over again.
	 * This grace period is required to prevent servers taking over each-others task records
	 * after a database was unavailable (i.e. servers might need some time to come 'back online').
	 * <br>Default 60 seconds. 
	 */
	public int getDbGracePeriodS() {
		return dbGracePeriodS;
	}
	/** See {@link #getDbGracePeriodS()} */
	public void setDbGracePeriodS(int dbGracePeriodS) {
		this.dbGracePeriodS = dbGracePeriodS;
	}

}

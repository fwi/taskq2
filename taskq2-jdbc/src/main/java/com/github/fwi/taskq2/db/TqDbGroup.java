package com.github.fwi.taskq2.db;

import java.sql.SQLException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import nl.fw.util.jdbc.DbConnNamedStatement;
import nl.fw.util.jdbc.DbConnUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.fwi.taskq2.TqBase;
import com.github.fwi.taskq2.TqEntry;
import com.github.fwi.taskq2.TqGroup;
import com.github.fwi.taskq2.db.poll.TqDbPoll;

public class TqDbGroup extends TqGroup {

	private static final Logger log = LoggerFactory.getLogger(TqDbGroup.class);

	private TaskDataSerializer serializer = new TaskDataSerializer();

	/**
	 * Cache-lock is used to clean the cache of task IDs no longer used.
	 */
	protected final Object cacheLock = new Object();
	protected ConcurrentHashMap<Long, AtomicInteger> taskIdRefs = new ConcurrentHashMap<>();

	private final TqDbServer dbServer;
	private TqDbPoll poller;

	public TqDbGroup(TqDbServer dbServer) {
		this.dbServer = dbServer;
	}

	public TqDbServer getDbServer() {
		return dbServer;
	}
	
	public TqDbConf getConf() {
		return getDbServer().getDbConf();
	}

	public TqDbPoll getPoller() {
		return poller;
	}

	public void setPoller(TqDbPoll poller) {
		this.poller = poller;
	}

	@Override
	protected void starting() {
		
		super.starting();
		if (poller != null) {
			poller.start();
		}
	}

	@Override
	protected void stopping(long taskFinishPeriodMs, long taskStopPeriodMs)  {
		
		if (poller != null) {
			poller.stop(taskFinishPeriodMs, taskStopPeriodMs);
		}
		super.stopping(taskFinishPeriodMs, taskStopPeriodMs);
	}

	public TaskDataSerializer getSerializer() { return serializer; }
	public void setSerializer(TaskDataSerializer serializer) { this.serializer = serializer; }

	@Override
	public boolean addTask(String qname, Object tdata, String qosKey) {
		return addTask(qname, tdata, qosKey, 0L);
	}

	public boolean addTask(String qname, Object tdata, long taskId) {
		return addTask(qname, tdata, null, taskId);
	}

	public boolean addTask(String qname, Object tdata, String qosKey, long taskId) {
		return addTask(qname, new TqEntry(tdata, qosKey, taskId));
	}

	/**
	 * Called by {@link com.github.fwi.taskq2.db.poll.LoadExpired} to check if a task is in a queue 
	 * before re-loading an expired task from the database.
	 * @return true if task is in a queue.
	 */
	public boolean containsTask(long taskId) {
		return (taskIdRefs.get(taskId) != null);
	}

	@Override
	public boolean addTask(String qname, TqEntry te) {

		if (te.getTaskId() == 0L) {
			throw new IllegalArgumentException("Task ID may not be 0.");
		}
		TqBase tq = getQueue(qname); 
		if (tq == null) {
			return false;
		}
		if (getConf().getMaxSize() > 0 && getConf().getMaxSize() < getSize()) {
			return false;
		}
		if (getConf().getMaxSizePerQ() > 0 && getConf().getMaxSizePerQ() < tq.getSize()) {
			return false;
		}
		AtomicInteger refCount = addCacheTaskId(te.getTaskId());
		boolean queued = false;
		try {
			queued = super.addTask(qname, te); 
		} finally {
			if (!queued && refCount.decrementAndGet() < 1) {
				removeCacheTaskId(te.getTaskId(), refCount);
			}
		}
		return queued;
	}

	protected AtomicInteger addCacheTaskId(long taskId) {

		AtomicInteger refCount = null;
		synchronized(cacheLock) {
			if ((refCount = taskIdRefs.get(taskId)) == null) {
				refCount = new AtomicInteger();
				taskIdRefs.put(taskId, refCount);
			}
			refCount.incrementAndGet();
		}
		return refCount;
	}

	protected void removeCacheTaskId(long taskId, AtomicInteger refCount) {

		synchronized(cacheLock) {
			if (refCount.get() < 1) {
				taskIdRefs.remove(taskId);
			}
		}
	}

	@Override
	protected void taskDone(TqBase tq, TqEntry te) {

		final long taskId = te.getTaskId();
		AtomicInteger refCount = taskIdRefs.get(taskId);
		if (refCount == null) {
			log.warn("Task {} is not present in task-ID cache.", taskId);
		} else {
			if (refCount.decrementAndGet() < 1) {
				removeCacheTaskId(taskId, refCount);
				if (log.isTraceEnabled()) {
					log.trace("Task {} removed from task-ID cache.", taskId);
				}
			}
		}
		super.taskDone(tq, te);
	}

	/**
	 * Calls {@link #storeTask(DbConnNamedStatement, String, Object, String)} with values from the prepared {@link TqEntry}.
	 * Sets the {@link TqEntry#getTaskId()} to the value returned from the {@link TqDbEntry}.
	 */
	public void storeTask(DbConnNamedStatement<?> c, String qname, TqEntry tqEntry) throws SQLException {
		
		TqDbEntry dbEntry = storeTask(c, qname, tqEntry.getTaskData(), tqEntry.getQosKey());
		tqEntry.setTaskId(dbEntry.getTaskId());
	}

	/**
	 * Stores a task with (serializable) item in the database. 
	 * <br>Does not commit or close the connection.
	 * @param c database connection
	 * @param qname queue name
	 * @param taskData the task data to persist (serialized using {@link #getSerializer()}).
	 * @param qosKey the (optional) Qos key for the task.
	 * @return the task data used to insert the task record in the database
	 */
	public TqDbEntry storeTask(DbConnNamedStatement<?> c, String qname, Object taskData, String qosKey) throws SQLException {

		TqDbEntry dbData = new TqDbEntry(qname, qosKey);
		dbData.setServerId(getDbServer().getServerId());
		dbData.setTaskDataBytes(getSerializer().taskDataToBytesRe(taskData));
		dbData.setExpireDate(getExpireDate(qname));
		c.nameStatement(TqQueryNames.INSERT_TASK, true);
		c.getNamedStatement().setInt("serverId", dbData.getServerId());
		c.getNamedStatement().setString("qname", dbData.getQname());
		c.getNamedStatement().setTimestamp("expireDate", new java.sql.Timestamp(dbData.getExpireDate()));
		c.getNamedStatement().setString("qosKey", dbData.getQosKey());
		int rcount = c.executeUpdate().getResultCount();
		if (rcount != 1) {
			throw new SQLException("Expected to insert one task record but inserted " + rcount + " record(s).");
		}
		Long taskId = (c.getResultSet().next() ? c.getResultSet().getLong(1) : null);
		if (taskId == null) {
			throw new SQLException("Unable to retrieve task row ID after task insert.");
		}
		dbData.setTaskId(taskId);
		
		c.nameStatement(TqQueryNames.INSERT_ITEM, true);
		c.getNamedStatement().setLong("taskId", dbData.getTaskId());
		c.getNamedStatement().setBytes("item", dbData.getTaskDataBytes());
		rcount = c.executeUpdate().getResultCount();
		if (rcount != 1) {
			throw new SQLException("Expected to insert one item record but inserted " + rcount + " record(s).");
		}
		return dbData;
	}

	/**
	 * The expire time in milliseconds for new/updated task records:
	 * <br><tt>now + {@link TqDbConf#getExpireTimeS()} * 1000</tt>
	 */
	public long getExpireDate(String qname) {
		return (System.currentTimeMillis() + getDbServer().getDbConf().getExpireTimeS() * 1000L);
	}

	/**
	 * Loads a task with item from the database. 
	 * Does not convert the {@link TqDbEntry#getTaskDataBytes()} to a task data object.
	 * <br>Does not commit or close the connection.
	 * <br>See also {@link #loadAndEnqueueTask(DbConnNamedStatement, String, long)}.
	 */
	public TqDbEntry loadTask(DbConnNamedStatement<?> c, long taskId) throws SQLException {

		TqDbEntry dbData = new TqDbEntry();
		dbData.setTaskId(taskId);
		dbData.setServerId(getDbServer().getServerId());
		c.nameStatement(TqQueryNames.LOAD_TASK);
		c.getNamedStatement().setLong("id", dbData.getTaskId());
		c.getNamedStatement().setLong("serverId", dbData.getServerId());
		if (c.executeQuery().getResultSet().next()) {
			dbData.setQname(c.getResultSet().getString("qname"));
			dbData.setExpireDate(c.getResultSet().getTimestamp("expire_date").getTime());
			dbData.setQosKey(c.getResultSet().getString("qos_key"));
			dbData.setTaskDataBytes(c.getResultSet().getBytes("item"));
			dbData.setRetryCount(c.getResultSet().getInt("retry_count"));
		}
		return (dbData.getQname() == null ? null : dbData);
	}
	
	/**
	 * Converts a {@link TqDbEntry} to a {@link TqEntry} using the {@link #getSerializer()}.
	 * @return null if dbData is null, else a {@link TqEntry}
	 * @throws RuntimeException if conversion from task-bytes to task-object fails.
	 */
	public TqEntry toTqEntry(TqDbEntry dbData) {
		
		if (dbData == null) {
			return null;
		}
		Object tdata = null;
		try {
			tdata = getSerializer().bytesToTaskData(dbData.getTaskDataBytes());
		} catch (Exception e) {
			DbConnUtil.rethrowRuntime(e);
		}
		return new TqEntry(tdata, dbData.getQosKey(), dbData.getTaskId());
	}

	/**
	 * Called by {@link com.github.fwi.taskq2.db.poll.LoadExpired}.
	 * Closes the given database connection after loading the task.
	 * <br>Commits and closes the connection.
	 * @return true if task is loaded and enqueued.
	 * @throws RuntimeException if task loading fails or conversion from task-bytes to task-object fails.
	 */
	public boolean loadAndEnqueueTask(DbConnNamedStatement<?> c, String qname, long taskId) {

		TqDbEntry dbData = null;
		try {
			dbData = loadTask(c, taskId);
			c.commitAndClose();
		} catch (Exception e) {
			c.rollbackAndClose(e);
		}
		if (dbData == null) {
			return false;
		}
		return addTask(dbData.getQname(), toTqEntry(dbData));
	}

	/**
	 * Delete a task from the database.
	 * <br>Does not commit or close the connection.
	 */
	public void deleteTask(DbConnNamedStatement<?> c, long taskId) throws SQLException {

		c.nameStatement(TqQueryNames.DELETE_TASK);
		c.getNamedStatement().setLong("id", taskId);
		c.getNamedStatement().setInt("serverId", getDbServer().getServerId());
		c.executeUpdate();
		if (c.getResultCount() != 1) {
			throw new SQLException("Expected to delete 1 task record, but deleted " + c.getResultCount());
		}
	}

	/**
	 * Abandon a task: the task will not be reloaded by the database poller.
	 * <br>Does not commit or close the connection.
	 */
	public void abandonTask(DbConnNamedStatement<?> c, long taskId) throws SQLException {

		c.nameStatement(TqQueryNames.ABANDON_TASK);
		c.getNamedStatement().setLong("id", taskId);
		c.getNamedStatement().setInt("serverId", getDbServer().getServerId());
		c.executeUpdate();
		if (c.getResultCount() != 1) {
			throw new SQLException("Expected to abandon 1 task record, but updated " + c.getResultCount());
		}
	}

	/**
	 * Update only the queue-name for a task, the task's item data is not updated.
	 * <br>Does not commit or close the connection.
	 */
	public void updateTaskQname(DbConnNamedStatement<?> c, String qname, long taskId) throws SQLException {

		c.nameStatement(TqQueryNames.UPDATE_TASK_QNAME);
		c.getNamedStatement().setLong("id", taskId);
		c.getNamedStatement().setInt("serverId", getDbServer().getServerId());
		c.getNamedStatement().setTimestamp("expireDate", new java.sql.Timestamp(getExpireDate(qname)));
		c.getNamedStatement().setString("qname", qname);
		c.executeUpdate();
		if (c.getResultCount() != 1) {
			throw new SQLException("Expected to update 1 task record for queue " + qname + ", but updated " + c.getResultCount());
		}
	}

	/**
	 * Update only the retry-count and the expire-date for a task, the task-item data is not updated.
	 * <br>Does not commit or close the connection.
	 * @param retryDelta the amount to add or substract from the retryCount.
	 * @return an updated {@link TqDbEntry} record for the given taskId.
	 */
	public TqDbEntry updateTaskRetry(DbConnNamedStatement<?> c, long taskId, int retryDelta) throws SQLException {

		TqDbEntry dbData = loadTask(c, taskId);
		dbData.setRetryCount(dbData.getRetryCount() + retryDelta);
		dbData.setExpireDate(getExpireDate(dbData.getQname()));
		c.nameStatement(TqQueryNames.UPDATE_TASK_RETRY);
		c.getNamedStatement().setLong("id", dbData.getTaskId());
		c.getNamedStatement().setInt("serverId", dbData.getServerId());
		c.getNamedStatement().setInt("retryCount", dbData.getRetryCount());
		c.getNamedStatement().setTimestamp("expireDate", new java.sql.Timestamp(dbData.getExpireDate()));
		c.executeUpdate();
		if (c.getResultCount() != 1) {
			throw new SQLException("Expected to update 1 task record for retry, but updated " + c.getResultCount());
		}
		return dbData;
	}

}

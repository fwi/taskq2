package com.github.fwi.taskq2.db.poll;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import javax.sql.DataSource;

import org.slf4j.Logger;

import com.github.fwi.taskq2.db.TqDbGroup;
import com.github.fwi.taskq2.db.TqQueryNames;

import nl.fw.util.jdbc.DbConn;
import nl.fw.util.jdbc.DbConnNamedStatement;

/**
 * The database polling classes assume the use of a connection pool.
 * @author fred
 *
 */
public class TqDbPollDao {

	private final TqDbGroup tgroup;
	private final DataSource ds;
	
	public TqDbPollDao(TqDbGroup tgroup, DataSource ds) {
		super();
		this.tgroup = tgroup;
		this.ds = ds;
	}
	
	public DbConnNamedStatement<?> createDbc() {
		
		DbConn dbc = new DbConn();
		dbc.setDataSource(ds);
		dbc.setNamedQueries(tgroup.getDbServer().getDbConf().getNamedQueries());
		return dbc;
	}

	public TqDbGroup getTaskGroup() { return tgroup; }

	public List<String> getExpiredQueues() throws SQLException {
		
		DbConnNamedStatement<?> c = createDbc();
		List<String> expiredQueues = new ArrayList<String>();
		try {
			c.nameStatement(TqQueryNames.HAVE_EXPIRED);
			c.getNamedStatement().setInt("serverId", tgroup.getDbServer().getServerId());
			c.getNamedStatement().setQueryTimeout(60);
			c.executeQuery();
			while (c.getResultSet().next()) {
				expiredQueues.add(c.getResultSet().getString("qname"));
			}
		} finally {
			c.close();
		}
		return expiredQueues;
	}
	
	public long getActiveTasksCount() throws SQLException {
		
		DbConnNamedStatement<?> c = createDbc();
		long count = 0L;
		try {
			c.nameStatement(TqQueryNames.TASKS_ACTIVE);
			c.getNamedStatement().setInt("serverId", tgroup.getDbServer().getServerId());
			c.getNamedStatement().setQueryTimeout(30);
			c.executeQuery();
			if (!c.getResultSet().next()) {
				throw new SQLException("Expected at least one record with task count.");
			}
			count = c.getResultSet().getLong("tasks_count");
		} finally {
			c.close();
		}
		return count;
	}
	
	public long getExpiredTask(DbConnNamedStatement<?> c, String qname) throws SQLException {
		
		long taskId = -1L;
		try {
			c.nameStatement(TqQueryNames.EXPIRED_PER_Q);
			c.getNamedStatement().setInt("serverId", tgroup.getDbServer().getServerId());
			c.getNamedStatement().setString("qname", qname);
			c.getNamedStatement().setInt("maxAmount", 1);
			c.getNamedStatement().getStatement().setQueryTimeout(60);
			c.executeQuery();
			while (c.getResultSet().next()) {
				if (taskId > -1L) {
					throw new SQLException("Expected only one expired task record.");
				}
				taskId = c.getResultSet().getLong("id");
			}
		} finally {
			c.close();
		}
		return taskId;
	}

	public void updateExpired(DbConnNamedStatement<?> c, String qname, long taskId) throws SQLException {
		
		try {
			c.nameStatement(TqQueryNames.UPDATE_EXPIRED);
			c.getNamedStatement().setTimestamp("expireDate", 
					new java.sql.Timestamp(tgroup.getExpireDate(qname)));
			c.getNamedStatement().setLong("id", taskId);
			c.getNamedStatement().setInt("serverId", tgroup.getDbServer().getServerId());
			c.getNamedStatement().setQueryTimeout(15);
			int updated = c.executeUpdate().getResultCount();
			if (updated != 1) {
				throw new SQLException("Failed to update expired task [" + taskId + "], updated " + updated + " records.");
			}
			c.commitAndClose();
		} finally {
			c.close();
		}
	}
	
	public List<Integer> getDeadServers(DbConnNamedStatement<?> c, int serverId) {
		
		List<Integer> serverIds = new ArrayList<>();
		try {
			c.nameStatement(TqQueryNames.DEAD_SERVERS);
			c.getNamedStatement().setString("group", tgroup.getDbServer().getDbConf().getServerGroup());
			c.getNamedStatement().setTimestamp("lastActive", 
					new java.sql.Timestamp(System.currentTimeMillis() + tgroup.getDbServer().getDbConf().getFailOverTimeoutS() * 1000L));
			c.getNamedStatement().setQueryTimeout(15);
			c.executeQuery();
			while (c.getResultSet().next()) {
				int deadServerId = c.getResultSet().getInt("id");
				if (deadServerId != serverId) { // never fail-over self.
					serverIds.add(deadServerId);
				}
			}
			c.commitAndClose();
		} catch (Exception e) {
			c.rollbackAndClose(e);
		}
		return serverIds;
	}

	public void takeOver(DbConnNamedStatement<?> c, int deadServerId, int serverId, Logger log) {
		
		log.info("Starting fail-over of dead server {}", deadServerId);
		try {
			c.nameStatement(TqQueryNames.LOCK_SERVER);
			c.getNamedStatement().setQueryTimeout(10);
			c.getNamedStatement().setInt("deadServerId", deadServerId);
			if (!c.executeQuery().getResultSet().next()) {
				throw new RuntimeException("Dead server is already abandoned.");
			}
		} catch (Exception e) {
			c.rollbackAndClose();
			// Not an error: another server might have gotten lock on server-record.
			log.warn("Failed to to get a lock on dead server record " + deadServerId + ": " + e);
			return;
		}
		log.debug("Locked taskq server {} for fail-over.", deadServerId);
		int movedTasks = 0;
		try {
			c.nameStatement(TqQueryNames.UPDATE_FAIL_OVER);
			c.getNamedStatement().setInt("serverId", serverId);
			c.getNamedStatement().setInt("deadServerId", deadServerId);
			movedTasks = c.executeUpdate().getResultCount();
			log.info("Moved " + movedTasks + " task records from dead server " + deadServerId + " to this server with ID " + serverId);
		} catch (Exception e) {
			c.rollbackAndClose();
			log.error("Could not update task records from dead server " + deadServerId, e);
			return;
		}
		try {
			c.nameStatement(TqQueryNames.ABANDON_DEAD_SERVER);
			c.getNamedStatement().setInt("deadServerId", deadServerId);
			c.executeUpdate(); 
			if (c.getResultCount() != 1) {
				log.warn("Marking dead server {} as abandoned resulted in {} records updated.", deadServerId, c.getResultCount());
			}
			c.commitAndClose();
			log.info("Completed fail over of " + movedTasks + " task records from abandoned dead server " + deadServerId);
		} catch (Exception e) {
			c.rollbackAndClose();
			log.error("Could not update dead server record " + deadServerId + ", all updates for task records have been reverted.", e);
		}
	}

}

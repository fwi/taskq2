package com.github.fwi.taskq2.db.itest;

import static org.junit.Assert.*;

import java.util.concurrent.TimeUnit;

import junit.framework.AssertionFailedError;
import nl.fw.util.jdbc.DbConn;
import nl.fw.util.jdbc.DbConnUtil;
import nl.fw.util.jdbc.hikari.HikPool;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.fwi.taskq2.db.DbTestTask;
import com.github.fwi.taskq2.db.TqDbConf;
import com.github.fwi.taskq2.db.TqDbEntry;
import com.github.fwi.taskq2.db.TqDbGroup;
import com.github.fwi.taskq2.db.TqDbInit;
import com.github.fwi.taskq2.db.TqDbServer;
import com.github.fwi.taskq2.db.TqQueryNames;
import com.github.fwi.taskq2.db.poll.DbTestGroup;
import com.github.fwi.taskq2.db.poll.DbTestTaskHandler;
import com.github.fwi.taskq2.db.poll.TqDbPoll;
import com.github.fwi.taskq2.db.poll.TqDbTest;

@SuppressWarnings("unused")
public class UseExternalDb {

	/**
	 * This is an integration test, only works when database is ready.
	 * See taskq-db-struct-mysql.sql and taskq-db-struct-pgsql.sql 
	 */
	boolean runtest = true;
	
	enum ExternalDbType { MYSQL, PGSQL };
	
	// ExternalDbType dbToUse = ExternalDbType.MYSQL;
	ExternalDbType dbToUse = ExternalDbType.PGSQL;
	
	private static final Logger log = LoggerFactory.getLogger(UseExternalDb.class);

	private DbTestGroup tgroup;
	private DbConn c;
	
	@SuppressWarnings("resource")
	@Test
	public void useExternalDb() {
		
		if (!runtest) {
			return;
		}
		String dbProps = (dbToUse == ExternalDbType.MYSQL ? "db-mysql.properties" : "db-pgsql.properties");
		String dbQueryProps = (dbToUse == ExternalDbType.MYSQL ? "taskq-db-queries-mysql.sql" : "taskq-db-queries-pgsql.sql");
		HikPool dbPool = new HikPool();
		c = new DbConn();
		try {
			dbPool.open(dbPool.loadDbProps(dbProps));
			TqDbConf dbConf = new TqDbConf();
			TqDbInit initDb = new TqDbInit(dbConf);
			dbConf.setNamedQueries(initDb.loadNamedQueries("taskq-db-queries.sql", dbQueryProps));
			TqDbServer dbServer = new TqDbServer(dbConf);
			c = new DbConn(dbPool.getDataSource()).setNamedQueries(dbConf.getNamedQueries());
			dbServer.registerServer(c);
			c.close();
			checkMergeProp();
			tgroup = new DbTestGroup(dbServer);
			storeTask();
			TqDbPoll poller = new TqDbPoll(tgroup, c.getDataSource());
			tgroup.setPoller(poller);
			dbConf.setNoFailOver(true);
			dbConf.setHeartBeatIntervalMs(2000L);
			dbConf.setReloadIntervalMs(20L);
			dbConf.setExpireTimeS(0);
			DbTestTaskHandler.setFactoryDbConn(new DbConn(c.getDataSource()).setNamedQueries(c.getNamedQueries()));
			tgroup.addQueue(new TqDbTest("q1", DbTestTaskHandler.getFactory()));
			tgroup.addQueue(new TqDbTest("q2", DbTestTaskHandler.getFactory()));
			tgroup.start();
			loadExpired();
		} catch (Exception e) {
			log.error("External DB test failed.", e);
			fail();
		} finally {
			c.rollbackAndClose();
			DbConnUtil.closeSilent(tgroup);
			dbPool.close();
		}
	}
	
	void checkMergeProp() throws Exception {
		
		c.nameStatement(TqQueryNames.MERGE_PROP).getNamedStatement().setString("key", "test-key");
		c.getNamedStatement().setString("value", "test-value");
		assertEquals(1, c.executeUpdate().getResultCount());
		c.commitAndClose();
	}
	
	void storeTask() throws Exception {
		
		final String testTask = "it's a test";
		TqDbEntry dbEntry = tgroup.storeTask(c, "test", testTask, null);
		long taskId = dbEntry.getTaskId();
		c.commit();
		dbEntry = tgroup.loadTask(c, taskId);
		assertEquals(testTask, tgroup.getSerializer().bytesToTaskData(dbEntry.getTaskDataBytes()));
		tgroup.deleteTask(c, dbEntry.getTaskId());
		c.commitAndClose();
	}
	
	void loadExpired() throws Exception {
		
		Thread.sleep(200L); // process tasks from previous tests
		tgroup.setPaused("q1", true);
		DbTestTask tdata = new DbTestTask();
		// String tdataItem = tdata.item;
		TqDbEntry dbEntry = tgroup.storeTask(c, "q1", tdata, null);
		c.commitAndClose();
		tgroup.addTask("q1", tdata, dbEntry.getTaskId());
		tgroup.setPaused("q1", false);
		if (!tdata.deleted.await(1L, TimeUnit.SECONDS)) {
			throw new AssertionFailedError("Test task " + dbEntry.getTaskId() + " is not processed.");
		}
		dbEntry = tgroup.storeTask(c, "q1", new DbTestTask(), null);
		c.commitAndClose();
		if (awaitTask(tgroup.getTasksExecuted(), 1000L, 2)) {
			log.debug("Stored test task processed.");
		} else {
			throw new AssertionFailedError("Stored test task is not processed.");
		}
	}
	
	boolean awaitTask(long currentTaskCount, long timeoutMs, int amount) throws Exception {
		
		long tend = System.currentTimeMillis() + timeoutMs;
		boolean processed = false;
		do {
			if (tgroup.getTasksExecuted() >= currentTaskCount + amount) {
				processed = true;
				break;
			} else {
				Thread.sleep(20L);
			}
		} while (System.currentTimeMillis() < tend);
		return processed;
	}
	
}

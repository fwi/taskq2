package com.github.fwi.taskq2.db.poll;

import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.SQLException;
import java.util.concurrent.TimeUnit;

import nl.fw.util.jdbc.DbConnNamedStatement;
import nl.fw.util.jdbc.DbConnUtil;
import nl.fw.util.jdbc.hikari.DbConnHik;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.fwi.taskq2.db.DbTestTask;
import com.github.fwi.taskq2.db.TestDb;
import com.github.fwi.taskq2.db.TqDbConf;
import com.github.fwi.taskq2.db.TqDbServer;

public class TestDbPoll {

	private static final Logger log = LoggerFactory.getLogger(TestDbPoll.class);

	private static TestDb tdb;
	private static DbTestGroup tgroup;
	private static TqDbPoll poller;
	
	@BeforeClass
	public static void setupTqDb() {
		
		tdb = new TestDb();
		tdb.getDbConf().setUseHostNameAsServerName(false);
		TqDbServer tdbServer = new TqDbServer(tdb.getDbConf());
		try (DbConnHik dbc = tdb.getDbConn()) {
			tdbServer.registerServer(tdb.getDbConn());
		}
		tgroup = new DbTestGroup(tdbServer);
		poller = new TqDbPoll(tgroup, tdb.getDataSource());
		DbTestTaskHandler.setFactoryDbConn(poller.getDao().createDbc());
	}
	
	@AfterClass
	public static void stopTqDb() {
		
		DbConnUtil.closeSilent(poller);
		DbConnUtil.closeSilent(tgroup);
		DbConnUtil.closeSilent(tdb);
	}
	
	public static TqDbConf getConf() {
		return tgroup.getDbServer().getDbConf();
	}
	
	@Test
	public void heartBeatPoll() {
		
		log.info("### Test heart-beat");
		long lastActiveDelta = 0L;
		try {
			HeartBeat hb = new HeartBeat(tgroup, poller.getDao());
			hb.pollDb();
			int oldServerId = tgroup.getDbServer().getServerId();
			deleteServer(oldServerId);
			// check new server-record insert
			hb.pollDb();
			int serverId = tgroup.getDbServer().getServerId();
			assertNotEquals(oldServerId, serverId);
			// check update of existing server record
			long oldLastActive = getLastActive(serverId);
			Thread.sleep(20); // minimum time required to guarantee last-active is updated with a new value.
			hb.pollDb();
			long lastActive = getLastActive(serverId);
			assertNotEquals(oldLastActive, lastActive);
			lastActiveDelta = (System.currentTimeMillis() - tgroup.getDbServer().getDbLastAvailable());
		} catch (Exception e) {
			log.error("Heart beat test failed.", e);
			fail();
		}
		log.debug("Last active date delta: " + lastActiveDelta);
		assertTrue(lastActiveDelta < 20L);
	}
	
	private void deleteServer(int serverId) throws SQLException {
		
		try (DbConnNamedStatement<?> c = poller.getDao().createDbc()) {
			c.createStatement().executeUpdate("delete from taskq_servers where id=" + serverId);
			c.commitAndClose();
		}
	}

	private long getLastActive(int serverId) throws SQLException {
		
		long lastActive = 0L;
		try (DbConnNamedStatement<?> c = poller.getDao().createDbc()) {
			c.createStatement().executeQuery("select last_active from taskq_servers where id=" + serverId);
			c.getResultSet().next();
			lastActive = c.getResultSet().getTimestamp("last_active").getTime();
		}
		return lastActive;
	}

	@Test
	public void loadExpiredPoll() {
		
		log.info("### Test load-expired");
		LoadExpired le = null;
		TqDbTest q1 = new TqDbTest("q1", DbTestTaskHandler.getFactory()); 
		TqDbTest q2 = new TqDbTest("q2", DbTestTaskHandler.getFactory()); 
		try {
			tgroup.addQueue(q1);
			tgroup.addQueue(q2);
			getConf().setExpireTimeS(0);
			tgroup.start();
			tgroup.setPoller(poller);
			le = new LoadExpired(tgroup, poller.getDao());
			storeTask(new DbTestTask());
			// retrieve task from database
			le.pollDb();
			DbTestTask task = q1.getLastQueued();
			if (!task.updated.await(1L, TimeUnit.SECONDS)) {
				throw new RuntimeException("Task 1 was not updated.");
			}
			if (!task.deleted.await(1L, TimeUnit.SECONDS)) {
				throw new RuntimeException("Task 1 was not deleted.");
			}
		} catch (Exception e) {
			log.error("Load expired test failed.", e);
			fail();
		} finally {
			tgroup.setPoller(null);
			DbConnUtil.closeSilent(tgroup);
			tgroup.removeQueue("q1");
			tgroup.removeQueue("q2");
		}
	}

	private void storeTask(DbTestTask t) throws SQLException {
		
		try (DbConnNamedStatement<?> c = poller.getDao().createDbc()) {
			tgroup.storeTask(c, "q1", t, null);
			c.commitAndClose();
		}
	}

	@Test
	public void testFailOver() {
		
		log.info("### Test fail-over");
		LoadExpired le = null;
		TqDbTest q1 = new TqDbTest("q1", DbTestTaskHandler.getFactory()); 
		TqDbTest q2 = new TqDbTest("q2", DbTestTaskHandler.getFactory()); 
		try {
			tgroup.addQueue(q1);
			tgroup.addQueue(q2);
			getConf().setExpireTimeS(0);
			
			HeartBeat hb = new HeartBeat(tgroup, poller.getDao());
			// insert server record
			hb.pollDb();
			int oldServerId = tgroup.getDbServer().getServerId();
			// insert task record
			// setting expired time is not needed, fail-over records get epire_date "now"
			// tgroup.setExpireTimeS(0);
			storeTask(new DbTestTask());
			// abandon old server
			updateServerPort(1234);
			// new server-record
			hb.pollDb();
			int serverId = tgroup.getDbServer().getServerId();
			assertNotEquals(oldServerId, serverId);
			// fail-over task from old server
			getConf().setDbGracePeriodS(0);
			getConf().setFailOverTimeoutS(0);
			FailOver fv = new FailOver(tgroup, poller.getDao());
			fv.pollDb();
			// retrieve task from database
			tgroup.start();
			tgroup.setPoller(poller);
			le = new LoadExpired(tgroup, poller.getDao());
			le.pollDb();
			DbTestTask task = q1.getLastQueued();
			if (!task.updated.await(1L, TimeUnit.SECONDS)) {
				throw new RuntimeException("Task 1 was not updated.");
			}
			if (!task.deleted.await(1L, TimeUnit.SECONDS)) {
				throw new RuntimeException("Task 1 was not deleted.");
			}
		} catch (Exception e) {
			log.error("Fail over test failed.", e);
			fail();
		} finally {
			tgroup.setPoller(null);
			DbConnUtil.closeSilent(tgroup);
			tgroup.removeQueue("q1");
			tgroup.removeQueue("q2");
		}
	}

	private void updateServerPort(int port) throws SQLException {
		
		tgroup.getDbServer().getDbConf().setServerPort(1234);
		try (DbConnNamedStatement<?> c = poller.getDao().createDbc()) {
			tgroup.getDbServer().registerServer(c);
			c.commitAndClose();
		}
	}

}

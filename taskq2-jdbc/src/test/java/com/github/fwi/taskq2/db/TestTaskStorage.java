package com.github.fwi.taskq2.db;

import static org.junit.Assert.*;
import nl.fw.util.jdbc.DbConnUtil;
import nl.fw.util.jdbc.hikari.DbConnHik;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestTaskStorage {

	private static final Logger log = LoggerFactory.getLogger(TestTaskStorage.class);

	private static TestDb tdb;
	private static TqDbGroup tgroup;
	
	@BeforeClass
	public static void setupTqDb() {
		
		tdb = new TestDb();
		tdb.getDbConf().setUseHostNameAsServerName(false);
		TqDbServer tdbServer = new TqDbServer(tdb.getDbConf());
		try (DbConnHik dbc = tdb.getDbConn()) {
			tdbServer.registerServer(tdb.getDbConn());
		}
		tgroup = new TqDbGroup(tdbServer);
		/*
		SingletonTaskHandlerFactory thandler = new SingletonTaskHandlerFactory(new DbTestTaskHandler());
		TqFifo tq = new TqFifo("q1", thandler);
		tgroup.addQueue(tq);
		tq = new TqFifo("q2", thandler);
		tgroup.addQueue(tq);
		*/
	}
	
	@AfterClass
	public static void stopTqDb() {
		
		DbConnUtil.closeSilent(tdb);
	}
	
	@Test
	public void insertAndLoadTask() {
		
		DbTestTask task = new DbTestTask(tgroup, "tq");
		DbTestTask tcopy = null;
		try (DbConnHik c = tdb.getDbConn()){
			TqDbEntry dbEntry = tgroup.storeTask(c, task.qname, task, null);
			task.id = dbEntry.getTaskId();
			log.debug("Test task inserted.");
			c.commit();
			dbEntry = tgroup.loadTask(c, task.id);
			tcopy = (DbTestTask) tgroup.getSerializer().bytesToTaskDataRe(dbEntry.getTaskDataBytes());
			tcopy.id = dbEntry.getTaskId();
			c.commit();
		} catch (Exception e) {
			e.printStackTrace();
			fail();
		}
		assertEquals(task.item, tcopy.item);
		assertEquals(task.id, tcopy.id);
	}
	
	@Test
	public void updateTqName() {
		
		DbTestTask task = new DbTestTask(tgroup, "tq");
		String tqname = null;
		try (DbConnHik c = tdb.getDbConn()){
			TqDbEntry dbEntry = tgroup.storeTask(c, task.qname, task, null);
			task.id = dbEntry.getTaskId();
			task.qname = "tq2";
			tgroup.updateTaskQname(c, task.qname, task.id);
			log.debug("Test task qname updated.");
			c.commit();
			dbEntry = tgroup.loadTask(c, task.id);
			tqname = dbEntry.getQname();
			c.commit();
		} catch (Exception e) {
			e.printStackTrace();
			fail();
		}
		assertEquals("tq2", tqname);
	}

}

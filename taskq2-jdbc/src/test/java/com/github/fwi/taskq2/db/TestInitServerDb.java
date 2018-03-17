package com.github.fwi.taskq2.db;

import static org.junit.Assert.*;
import nl.fw.util.jdbc.DbConnUtil;
import nl.fw.util.jdbc.hikari.DbConnHik;
import nl.fw.util.jdbc.hikari.HikPool;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("unused")
public class TestInitServerDb {

	private static final Logger log = LoggerFactory.getLogger(TestInitServerDb.class);
	
	@Test
	public void testInitDb() {
		
		TestDb tdb = null;
		try {
			tdb = new TestDb();
		} catch (Exception e) {
			log.error("unable to initialize database", e);
			throw new AssertionError(e);
		} finally {
			DbConnUtil.closeSilent(tdb);
		}
	}

	@Test
	public void testDbServer() {
		
		TestDb tdb = null;
		try {
			tdb = new TestDb();
			TqDbServer tdbServer = new TqDbServer(tdb.getDbConf());
			try (DbConnHik dbc = tdb.getDbConn()) {
				tdbServer.registerServer(tdb.getDbConn());
				testPropQueries(dbc);
			}
			assertTrue("server ID updated", (tdbServer.getServerId() > 0));
		} catch (Exception e) {
			log.error("unable to initialize database server", e);
			throw new AssertionError(e);
		} finally {
			DbConnUtil.closeSilent(tdb);
		}
	}
	
	private void testPropQueries(DbConnHik dbc) throws Exception {
		
		dbc.nameStatement(TqQueryNames.GET_PROP).getNamedStatement().setString("key", "taskq.version");
		assertTrue("Have taskq version property", dbc.executeQuery().getResultSet().next());
		String version = dbc.getResultSet().getString(1);
		log.debug("TaskQ version: " + version);
		dbc.nameStatement(TqQueryNames.MERGE_PROP).getNamedStatement().setString("key", "test-key");
		dbc.getNamedStatement().setString("value", "test-value");
		assertEquals(1, dbc.executeUpdate().getResultCount());
		dbc.commit();
		dbc.nameStatement(TqQueryNames.GET_PROP).getNamedStatement().setString("key", "test-key");
		assertTrue(dbc.executeQuery().getResultSet().next());
		assertEquals("test-value", dbc.getResultSet().getString(1));
		dbc.nameStatement(TqQueryNames.MERGE_PROP).getNamedStatement().setString("key", "test-key");
		dbc.getNamedStatement().setString("value", "test-value2");
		assertEquals(1, dbc.executeUpdate().getResultCount());
		dbc.commit();
		dbc.nameStatement(TqQueryNames.GET_PROP).getNamedStatement().setString("key", "test-key");
		assertTrue(dbc.executeQuery().getResultSet().next());
		assertEquals("test-value2", dbc.getResultSet().getString(1));
		dbc.nameStatement(TqQueryNames.DELETE_PROP).getNamedStatement().setString("key", "test-key");
		assertEquals(1, dbc.executeUpdate().getResultCount());
		dbc.commit();
	}

}

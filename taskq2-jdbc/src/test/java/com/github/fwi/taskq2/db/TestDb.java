package com.github.fwi.taskq2.db;

import java.io.Closeable;
import java.io.IOException;

import javax.sql.DataSource;

import nl.fw.util.jdbc.DbConnUtil;
import nl.fw.util.jdbc.hikari.DbConnHik;
import nl.fw.util.jdbc.hikari.HikPool;

public class TestDb implements Closeable {

	private final boolean useVerboseDb = false;
	
	private final String testDbPrefix = "db.test2.";
	private final String testDbPrefixVerbose = "db.test.";
	private final String testDbPropsResource = "db-test.properties";
	
	private HikPool dbPool;
	private TqDbConf dbConf;
	
	public TestDb() {
		super();
		dbPool = new HikPool();
		try {
			dbPool.open(dbPool.loadDbProps(testDbPropsResource, (useVerboseDb ? testDbPrefixVerbose : testDbPrefix)));
		} catch (IOException e) {
			DbConnUtil.rethrowRuntime(e);
		}
		dbConf = new TqDbConf();
		TqDbInit initDb = new TqDbInit(dbConf);
		try (DbConnHik dbc = new DbConnHik(dbPool)) {
			initDb.initDb(dbc);
		}
	}
	
	public TqDbConf getDbConf() {
		return dbConf;
	}

	public DbConnHik getDbConn() {
		return new DbConnHik(dbPool, dbConf.getNamedQueries());
	}

	public DataSource getDataSource() {
		return dbPool.getDataSource();
	}

	@Override
	public void close() {
		
		if (dbPool != null) {
			dbPool.close();
			dbPool = null;
		}
	}

}

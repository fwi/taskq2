package com.github.fwi.taskq2.demo;

import com.github.fwi.taskq2.db.TqDbGroup;

import nl.fw.util.jdbc.DbConn;
import nl.fw.util.jdbc.DbConnNamedStatement;
import nl.fw.util.jdbc.hikari.HikPool;

/**
 * Helper class for shared variables between the Demo and TasksHandler class.
 */
public class DemoGroup {

	public final HikPool dbPool;
	public final TqDbGroup tgroup;
	
	public DemoGroup(HikPool dbPool, TqDbGroup tgroup) {
		this.dbPool = dbPool;
		this.tgroup = tgroup;
	}

	/**
	 * Utility method to get a database connection backed by the dbPool
	 * and with all named TaskQ-queries registered. 
	 */
	public DbConnNamedStatement<?> getDbConn() {
		
		DbConn dbc = new DbConn();
		dbc.setDataSource(dbPool.getDataSource());
		dbc.setNamedQueries(tgroup.getDbServer().getDbConf().getNamedQueries());
		return dbc;
	}

}

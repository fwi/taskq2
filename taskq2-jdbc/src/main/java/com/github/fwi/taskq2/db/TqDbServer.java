package com.github.fwi.taskq2.db;

import java.sql.Connection;
import java.sql.SQLException;

import nl.fw.util.jdbc.DbConn;
import nl.fw.util.jdbc.DbConnNamedStatement;
import nl.fw.util.jdbc.DbConnUtil;
import nl.fw.util.jdbc.INamedQuery;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TqDbServer {

	private static final Logger log = LoggerFactory.getLogger(TqDbServer.class);

	private volatile boolean dbAvailable;
	private volatile long dbLastAvailable = System.currentTimeMillis();
	private volatile long dbLastUnavailable = System.currentTimeMillis();
	protected volatile int serverId = -1;
	protected final TqDbConf dbConf;

	public TqDbServer(TqDbConf dbConf) {
		this.dbConf = dbConf;
		if (dbConf.isUseHostNameAsServerName()) {
			determineLocalHostname();
		}
	}
	
	public TqDbConf getDbConf() {
		return dbConf;
	}
	
	protected void determineLocalHostname() {
		
		try {
			java.net.InetAddress localHost = java.net.InetAddress.getLocalHost();
			String ip = localHost.getHostAddress();
			String name = localHost.getCanonicalHostName();
			if (ip.equals(name)) {
				name = localHost.getHostName();
			}
			dbConf.setServerHost(name);
			log.info("Found server host {}", name);
		} catch (Exception e) {
			log.warn("Could not use local hostname as server name - " + e);
		}
	}
	
	public String getServerName() {
		return dbConf.getServerHost() + ":" + dbConf.getServerPort();
	}
	
	public int getServerId() {
		return serverId;
	}
	
	@SuppressWarnings("resource")
	public void registerServer(Connection c, INamedQuery namedQueries) {
		registerServer(new DbConn(c).setNamedQueries(namedQueries));
	}
	
	/**
	 * Registers this server by updating an existing server-record for this server
	 * or inserting a new server-record for this server.
	 * <br>If this cannot be done, {@link #setDbAvailable(boolean)} is called with FALSE,
	 * otherwise the method is called with TRUE. 
	 * @param c A database connection, not closed by this method.
	 */
	public void registerServer(DbConnNamedStatement<?> c) {
		
		try {
			boolean exists = false;
			serverId = findServer(c);
			if (serverId > -1) {
				exists = true;
				updateServerActive(c);
			} else {
				serverId = insertServer(c);
			}
			c.commit();
			setDbAvailable(true);
			log.info("{} server ID {} for {} in group {}", (exists ? "Updated" : "Registered"), serverId, getServerName(), dbConf.getServerGroup());
		} catch (Exception e) {
			c.rollbackSilent();
			setDbAvailable(false);
			serverId = -1;
			DbConnUtil.rethrowRuntime(e);
		}
	}
	
	protected int findServer(DbConnNamedStatement<?> c) throws SQLException {
		
		c.nameStatement(TqQueryNames.FIND_SERVER);
		c.getNamedStatement().setString("name", getServerName());
		c.getNamedStatement().setString("group", dbConf.getServerGroup());
		c.executeQuery();
		return (c.getResultSet().next() ? c.getResultSet().getInt("id") : -1);
	}
	
	protected int insertServer(DbConnNamedStatement<?> c) throws SQLException {
		
		c.nameStatement(TqQueryNames.INSERT_SERVER, true);
		c.getNamedStatement().setString("name", getServerName());
		c.getNamedStatement().setString("group", dbConf.getServerGroup());
		c.getNamedStatement().setTimestamp("lastActive", getLastActive());
		c.executeUpdate();
		return (c.getResultSet().next() ? c.getResultSet().getInt(1) : -1);
	}

	protected int updateServerActive(DbConnNamedStatement<?> c) throws SQLException {
		
		c.nameStatement(TqQueryNames.SERVER_ACTIVE);
		c.getNamedStatement().setInt("id", serverId);
		c.getNamedStatement().setTimestamp("lastActive", getLastActive());
		c.getNamedStatement().setQueryTimeout(15);
		return c.executeUpdate().getResultCount();
	}

	protected java.sql.Timestamp getLastActive() {
		return new java.sql.Timestamp(System.currentTimeMillis());
	}

	@SuppressWarnings("resource")
	public boolean updateActive(Connection c, INamedQuery namedQueries) {
		return updateActive(new DbConn(c).setNamedQueries(namedQueries));
	}
	
	public boolean updateActive(DbConnNamedStatement<?> c) {
		
		int updated = 0;
		try {
			updated = updateServerActive(c);
			c.commit();
			setDbLastAvailable(System.currentTimeMillis());
		} catch (Exception e) {
			c.rollbackSilent();
			DbConnUtil.rethrowRuntime(e);
		}
		return (updated == 1);
	}
	
	public boolean isDbAvailable() {
		return dbAvailable;
	}
	
	public void setDbAvailable(boolean dbAvailable) {
		
		this.dbAvailable = dbAvailable;
		if (dbAvailable) {
			setDbLastAvailable(System.currentTimeMillis());
		} else {
			setDbLastUnavailable(System.currentTimeMillis());
		}
	}
	
	public long getDbLastAvailable() {
		return dbLastAvailable;
	}
	
	public void setDbLastAvailable(long dbLastAvailable) {
		this.dbLastAvailable = dbLastAvailable;
	}

	public long getDbLastUnavailable() {
		return dbLastUnavailable;
	}

	public void setDbLastUnavailable(long dbLastUnavailable) {
		this.dbLastUnavailable = dbLastUnavailable;
	}
}

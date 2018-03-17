package com.github.fwi.taskq2.db;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URI;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import nl.fw.util.jdbc.DbConn;
import nl.fw.util.jdbc.DbConnNamedStatement;
import nl.fw.util.jdbc.DbConnUtil;
import nl.fw.util.jdbc.NamedQuery;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class to initialize the TaskQ database.
 * <br>Resource strings starting with "file:" are treated as a URI.
 * <br>Methods in this class do NOT close a given database connection.
 * @author fred
 *
 */
public class TqDbInit {

	private static final Logger log = LoggerFactory.getLogger(TqDbInit.class);

	protected boolean wasInitialized;
	protected TqDbConf dbConf;
	
	public TqDbInit() {}
	public TqDbInit(TqDbConf dbConf) {
		setDbConf(dbConf);
	}
	
	public TqDbConf getDbConf() {
		return dbConf;
	}
	public void setDbConf(TqDbConf dbConf) {
		this.dbConf = dbConf;
	}
	
	public void initDb(Connection c) {
		initDb(new DbConn(c));
	}

	public void initDb(DbConnNamedStatement<?> c) {
		
		if (getDbConf() == null) {
			throw new IllegalStateException("TaskQ database configuration must be set.");
		}
		if (dbConf.getNamedQueries() == null) {
			loadNamedQueries();
		}
		initStruct(c);
	}
	
	public boolean wasInitialized() {
		return wasInitialized;
	}

	public NamedQuery loadNamedQueries() {
		return loadNamedQueries(getDbConf().getNamedQueriesResource());
	}
	
	public NamedQuery loadNamedQueries(String... resources) {
		
		NamedQuery nq = null;
		InputStream in = null;
		try {
			Map<String, String> namedQueriesMap = new HashMap<>();
			for (String resource : resources) {
				in = getInputStream(resource);
				namedQueriesMap.putAll(NamedQuery.loadQueries(getReader(in)));
				in.close();
				in = null;
			}
			nq = new NamedQuery(namedQueriesMap);
			if (TqQueryNames.GET_PROP.equals(nq.getQuery(TqQueryNames.GET_PROP))) {
				throw new IllegalArgumentException("Queries loaded from [" + Arrays.toString(resources) + "] do not contain named TaskQ queries.");
			}
			dbConf.setNamedQueries(nq);
		} catch (Exception e) {
			DbConnUtil.rethrowRuntime(e);
		} finally {
			DbConnUtil.closeSilent(in);
		}
		log.debug("TaskQ named queries loaded from {}", Arrays.toString(resources));
		return nq;
	}

	protected InputStream getInputStream(String resourceName) throws Exception {
		
		InputStream in = null;
		if (resourceName.startsWith("file:")) {
			in = new URI(resourceName).toURL().openStream();
		} else {
			in = DbConnUtil.getResourceAsStream(resourceName);
		}
		if (in == null) {
			throw new FileNotFoundException("Cannot find resource to open: " + resourceName);
		}
		return in;
	}
	
	protected Reader getReader(InputStream in) {
		return new BufferedReader(
				new InputStreamReader(in, Charset.forName(getDbConf().getDbResourceCharsetName())));
	}

	public void initStruct(Connection conn) {
		initStruct(new DbConn(conn));
	}
	
	public void initStruct(DbConnNamedStatement<?> c) {
		
		c.setNamedQueries(dbConf.getNamedQueries());
		if (isAlreadyInitialized(c)) {
			log.debug("TaskQ database already initialized.");
			return;
		}
		log.debug("Initializing TaskQ database.");
		InputStream in = null;
		try {
			in = getInputStream(getDbConf().getDbStructResource());
			loadAndUpdate(c, in);
			in.close();
			in = getInputStream(getDbConf().getDbInitQueriesResource());
			loadAndUpdate(c, in);
			wasInitialized = true;
		} catch (Exception e) {
			DbConnUtil.rethrowRuntime(e);
		} finally {
			DbConnUtil.closeSilent(in);
		}
		log.debug("TaskQ database initialized.");
	}
	
	/**
	 * Performs a "get property value" query to see if the database already has TaskQ tables.
	 */
	protected boolean isAlreadyInitialized(DbConnNamedStatement<?> c) {
		
		boolean haveStruct = false;
		try {
			c.nameStatement(TqQueryNames.GET_PROP).getNamedStatement().setString("key", "init-test");
			c.executeQuery();
			c.commit();
			haveStruct = true;
		} catch (Exception ignored) {
			// there is no structure
			c.rollbackSilent();
		}
		return haveStruct;
	}
	
	protected void loadAndUpdate(DbConnNamedStatement<?> c, InputStream in) {
		
		try {
			LinkedHashMap<String, String> qmap = NamedQuery.loadQueries(getReader(in));
			for (Map.Entry<String, String> sqlEntry : qmap.entrySet()) {
				log.trace("Executing sql query {}", sqlEntry.getKey());
				c.createStatement().executeUpdate(sqlEntry.getValue());
			}
			c.commit();
		} catch (Exception e) {
			c.rollbackSilent();
			DbConnUtil.rethrowRuntime(e);
		}
	}

}

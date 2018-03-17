package com.github.fwi.taskq2.db.poll;

import java.util.List;

import com.github.fwi.taskq2.db.TqDbConf;
import com.github.fwi.taskq2.db.TqDbGroup;
import com.github.fwi.taskq2.db.TqDbServer;

import nl.fw.util.jdbc.DbConnNamedStatement;

/**
 * The fail over poller checks for dead servers (using {@link TqDbConf#getFailOverTimeoutS()}) 
 * and moves non-abandoned taskq records to this task group for execution.
 * <br>The database is used to lock a dead server taskq-record so that only one server can fail over records from a dead server.
 * To prevent fail over of servers after a database outage, the {@link TqDbServer#getDbLastAvailable()} is checked 
 * (value is updated by the {@link HeartBeat} poller) and used together with the {@link TqDbConf#getDbGracePeriodS()}.
 *
 */
public class FailOver extends DbPollTask {
	
	public FailOver(TqDbGroup tgroup, TqDbPollDao dao) {
		super(tgroup, dao);
	}

	@Override
	public long getPollIntervalMs() {
		return (getConf().getFailOverTimeoutS() * 1000) / 2;
	}

	protected TqDbConf getConf() { 
		return tgroup.getDbServer().getDbConf();
	}

	public boolean isInDbGracePeriod() {
		return (System.currentTimeMillis() < tgroup.getDbServer().getDbLastUnavailable() + getConf().getDbGracePeriodS() * 1000L);
	}

	@Override
	public void pollDb() throws Exception {
		
		if (!tgroup.getDbServer().isDbAvailable() || isInDbGracePeriod()) {
			return;
		}
		final DbConnNamedStatement<?> c = dao.createDbc();
		final int serverId = tgroup.getDbServer().getServerId();
		List<Integer> serverIds = dao.getDeadServers(c, serverId);
		if (serverIds.isEmpty()) {
			log.trace("No inactive servers found.");
			return;
		}
		if (log.isDebugEnabled()) {
			log.debug("Found " + serverIds.size() + " dead servers: " + serverIds);
		}
		for (int deadServerId : serverIds) {
			dao.takeOver(c, deadServerId, serverId, log);
		}
	}

}

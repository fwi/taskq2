package com.github.fwi.taskq2.db.poll;

import com.github.fwi.taskq2.db.TqDbConf;
import com.github.fwi.taskq2.db.TqDbGroup;
import com.github.fwi.taskq2.db.TqDbServer;

import nl.fw.util.jdbc.DbConnNamedStatement;

/**
 * The heart beat poller updates the "last active" date for the current server.
 * If the last active date is not updated, fail over might be triggered (see {@link FailOver}).
 * <br>If the last active date cannot be updated, {@link TqDbServer#setDbAvailable(boolean)} is updated
 * and if {@link TqDbConf#isNoHeartBeatPause()} is false, the task execution for the task group is paused
 * until the database is available again.
 *
 */
public class HeartBeat extends DbPollTask {

	private volatile boolean wasPausedByHeartBeat;

	public HeartBeat(TqDbGroup tgroup, TqDbPollDao dao) {
		super(tgroup, dao);
	}

	@Override
	public long getPollIntervalMs() {
		return tgroup.getDbServer().getDbConf().getHeartBeatIntervalMs();
	}

	@Override
	public void pollDb() {

		final TqDbServer dbServer = tgroup.getDbServer();
		boolean wasDbAvailable = dbServer.isDbAvailable();
		final DbConnNamedStatement<?> c = dao.createDbc();
		try {
			if (dbServer.updateActive(c)) {
				log.debug("Updated last active date for server {}", dbServer.getServerId());
			} else {
				throw new Exception("No last active date updated for server " + dbServer.getServerId());
			}
		} catch (Exception e) {
			if (wasDbAvailable) {
				log.info("Last active date for server {} could not be updated: {}", dbServer.getServerId(), e.toString());
			}
			try {
				dbServer.registerServer(c);
			} catch (Exception e2) {
				if (wasDbAvailable) {
					log.error("Database unavailable, could not register last active date for server {}.", dbServer.getServerId(), e2);
				} else {
					log.debug("Database remains unavailable.");
				}
			}
		} finally {
			c.close();
		}
		// the value for dbServer.isDbAvailable() is updated by dbServer.registerServer(c)
		if (dbServer.isDbAvailable() && !wasDbAvailable) {
			// database OK again
			if (wasPausedByHeartBeat) {
				log.info("Database available, continuing task execution.");
				tgroup.setPaused(false);
			} else {
				log.info("Database available.");
			}
		}
		if (!dbServer.isDbAvailable() && wasDbAvailable) {
			// database has gone fishing
			if (tgroup.isPaused()) {
				wasPausedByHeartBeat = false;
			} else if (!tgroup.getConf().isNoHeartBeatPause()) {
				tgroup.setPaused(true);
				wasPausedByHeartBeat = true;
				log.info("Task execution is paused, waiting for database to become available.");
			}
		}
	}

}

package com.github.fwi.taskq2.db.poll;

import java.sql.SQLException;
import java.util.List;

import com.github.fwi.taskq2.TqBase;
import com.github.fwi.taskq2.db.TqDbConf;
import com.github.fwi.taskq2.db.TqDbGroup;

import nl.fw.util.jdbc.DbConnNamedStatement;

/**
 * The load expired poller reloads taskq records which have not been updated (using {@link TqDbConf#getExpireTimeS()}).
 * <br>If a task fails to execute, the taskq record is not updated and this poller will then reload the task
 * provided the task is not queued in memory (this is checked using {@link TqDbGroup#containsTask(long)}).
 * Tasks are only reloaded when there is room to reload tasks, this depends on the {@link TqDbConf#getMaxSize()} 
 * and {@link TqDbConf#getMaxSizePerQ()} settings in addition to:
 * <br> - {@link TqDbConf#getDbReloadMinFree()} and
 * <br> - {@link TqDbConf#getDbReloadMaxAmount()}.
 * <br>Reloaded tasks are enqueud using {@link TqDbGroup#loadAndEnqueueTask(DbConnNamedStatement, String, long)}
 * after their expired-date is updated.
 *
 */
public class LoadExpired extends DbPollTask {
	
	public LoadExpired(TqDbGroup tgroup, TqDbPollDao dao) {
		super(tgroup, dao);
	}

	@Override
	public long getPollIntervalMs() {
		return getConf().getReloadIntervalMs();
	}

	protected TqDbConf getConf() { 
		return tgroup.getConf();
	}
	
	protected boolean haveRoom(int size, int maxSize) {
		
		if (maxSize > 0) { 
			int minFree = (int)(maxSize * (getConf().getDbReloadMinFree() / 100.0d));
			return (size + minFree < maxSize);
		} else {
			return true;
		}
	}

	public boolean haveRoom() {
		return haveRoom(tgroup.getSize(), getConf().getMaxSize());
	}

	public boolean haveRoom(TqBase q) {
		return (haveRoom() && haveRoom(q.getSize(), getConf().getMaxSizePerQ()));
	}

	@Override
	public void pollDb() throws Exception {
		
		if (tgroup.isPaused() || !tgroup.getDbServer().isDbAvailable() || isStopping()) {
			return;
		}
		if (!haveRoom(tgroup.getSize(), getConf().getMaxSize())) {
			log.debug("Skipping check for expired records, maximum size reached.");
			return;
		}
		List<String> expiredQueues = dao.getExpiredQueues();
		if (expiredQueues.isEmpty()) {
			log.trace("No task queues with expired records.");
		} else {
			log.debug("Following task queues have expired records: {}", expiredQueues);
			reloadExpired(expiredQueues);
		}
	}
	
	private void reloadExpired(List<String> expiredQueues) throws SQLException {
		
		DbConnNamedStatement<?> c = dao.createDbc();
		for (String qname : expiredQueues) {
			reloadExpired(c, qname);
		}
	}
	
	private void reloadExpired(DbConnNamedStatement<?> c, String qname) throws SQLException {
		
		final TqBase q = tgroup.getQueue(qname);
		if (q == null) {
			log.warn("Cannot load expired records for unavailable queue {}.", qname);
			return;
		}
		if (q.isPaused()) {
			log.debug("Skipping reloading tasks for paused queue {}", qname);
			return;
		}
		if (!haveRoom(q)) {
			log.debug("No room for reloading tasks in queue {}", qname);
			return;
		}
		int reloaded = 0;
		int updated = 0;
		// always check "isStopping": in case of trouble, reload expired will be very active,
		// and at the same time, the whole TaskQ server can be stopped to prevent more touble and fix the problem.
		reloadLoop: while (haveRoom(q) && !q.isPaused() && !isStopping()) {
			if (getConf().getDbReloadMaxAmount() > 0 && reloaded >= getConf().getDbReloadMaxAmount()) {
				break reloadLoop;
			}
			if (getConf().getDbReloadLogAmount() > 0 && reloaded + updated > 0 && (reloaded + updated) % getConf().getDbReloadLogAmount() == 0) {
				log.debug("Reload expired tasks progress for queue {} at {} tasks reloaded and {} tasks updated.", qname, reloaded, updated);
			}
			long taskId = dao.getExpiredTask(c, qname);
			if (taskId < 0L) {
				break reloadLoop;
			}
			try {
				dao.updateExpired(c, qname, taskId);
			} catch (SQLException e) {
				// Most likely, record got modified/deleted.
				// This should not happen and indicates the queue is actively working on records that we try to reload here. 
				log.warn("[{}] Unable to update expired date on task in queue {}: {}", taskId, qname, e.toString());
				break reloadLoop;
			}
			if (tgroup.containsTask(taskId)) {
				updated++;
				log.trace("[{}] Updated expired date for task in queue {}.", taskId, qname);
				continue reloadLoop;
			} else {
				try {
					if (!tgroup.loadAndEnqueueTask(c, qname, taskId)) {
						log.warn("[{}] Unable to reload expired task in queue {} .", taskId, qname);
						break reloadLoop;
					}
					reloaded++;
					log.trace("[{}] Reloaded task in queue {}.", taskId, qname);
				} catch (Exception e) {
					log.error("[" + taskId + "] Failed to load task from queue [" + qname + "]", e);
					break reloadLoop;
				}
			}
		}
		if (reloaded + updated == 0) {
			log.debug("No tasks reloaded or updated in queue {}.", qname);
		} else {
			log.debug("Reloaded {} task(s) and only updated expired date for {} task(s) in queue {}.", reloaded, updated, qname);
		}
	}
	
}

package com.github.fwi.taskq2.db.poll;

import java.util.concurrent.Callable;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.fwi.taskq2.db.TqDbGroup;

/**
 * Base class for the poller's.
 * This is a "self scheduling" task that can be stopped and removed from schedule.
 * By default, the stopping value from {@link TqDbPoll#isStopping()} is checked
 * (which is set to true when {@link TqDbGroup#stop()} is called).
 * @author fred
 *
 */
public abstract class DbPollTask implements Callable<Void> {

	protected Logger log = LoggerFactory.getLogger(this.getClass());
	
	protected final TqDbPollDao dao;
	protected final TqDbGroup tgroup;
	protected volatile ScheduledFuture<Void> scheduledPoll;

	public DbPollTask(TqDbGroup tgroup, TqDbPollDao dao) {
		super();
		this.tgroup = tgroup;
		this.dao = dao;
	}

	public ScheduledFuture<Void> getScheduledPoll() {
		return scheduledPoll;
	}
	
	public abstract long getPollIntervalMs();
	
	public void schedulePoll() {

		if (isStopping()) {
			logStopEvent();
		} else {
			scheduledPoll = tgroup.getPoller().getScheduledExecutorService().schedule(
					this, getPollIntervalMs(), TimeUnit.MILLISECONDS);
		}
	}
	
	protected void logStopEvent() {
		log.debug("Taskq database poller {} stopped.", getName());
	}

	public boolean isStopping() {
		// unit tests do not set a poller
		return (tgroup.getPoller() == null ? false : tgroup.getPoller().isStopping());
	}
	
	public String getName() {
		return this.getClass().getSimpleName();
	}
	
	@Override
	public Void call() {

		if (isStopping()) {
			logStopEvent();
			return null;
		}
		try {
			log.trace("Taskq database poller {} checking.", getName());
			pollDb();
		} catch (Exception e) {
			if (isStopping() && e instanceof InterruptedException) {
				log.debug("Taskq database poller {} interrupted at stop.", getName());
			} else {
				log.error("Taskq database poller {} failed.", getName(), e);
			}
		}
		if (isStopping()) {
			logStopEvent();
		} else {
			schedulePoll();
		}
		return null;
	}
	
	public abstract void pollDb() throws Exception;

}

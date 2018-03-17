package com.github.fwi.taskq2.db.poll;

import java.io.Closeable;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.fwi.taskq2.db.TqDbGroup;
import com.github.fwi.taskq2.util.ExecutorTerminator;
import com.github.fwi.taskq2.util.DaemonThreadPool.DaemonThreadFactory;

public class TqDbPoll implements Closeable {

	private static final Logger log = LoggerFactory.getLogger(TqDbPoll.class);
	public static final int POLLING_TASKS_AMOUNT = 3;
	
	private final TqDbGroup tgroup;
	private final TqDbPollDao dao;
	private volatile boolean stop;
	private HeartBeat heartBeatPoll;
	private LoadExpired expiredPoll;
	private FailOver failOverPoll;

	private ScheduledExecutorService scheduler;
	private boolean shutdownExecutorOnClose;

	public TqDbPoll(TqDbGroup tgroup, DataSource ds) {
		this.tgroup = tgroup;
		this.dao = new TqDbPollDao(tgroup, ds);
	}
	
	public TqDbGroup getGroup() { return tgroup; }
	public TqDbPollDao getDao() { return dao; }
	
	public ScheduledExecutorService getScheduledExecutorService() { return scheduler; }
	public void setScheduledExecutorService(ScheduledExecutorService scheduler) { this.scheduler = scheduler; }

	public boolean isShutdownExecutorOnClose() { return shutdownExecutorOnClose; }
	public void setShutdownExecutorOnClose(boolean shutdownExecutorOnClose) { this.shutdownExecutorOnClose = shutdownExecutorOnClose; }

	public boolean isStopping() { return stop; }

	public void start() {
		
		if (getScheduledExecutorService() == null) {
			setScheduledExecutorService(createScheduledExecutorService(POLLING_TASKS_AMOUNT, POLLING_TASKS_AMOUNT + 2));
			setShutdownExecutorOnClose(true);
		}
		stop = false;
		// start pollers
		getScheduledExecutorService().submit(heartBeatPoll = new HeartBeat(tgroup, dao));
		getScheduledExecutorService().submit(expiredPoll = new LoadExpired(tgroup, dao));
		if (!getGroup().getConf().isNoFailOver()) {
			getScheduledExecutorService().submit(failOverPoll = new FailOver(tgroup, dao));
		}
		log.info("Taskq database poller started.");
	}

	@Override
	public void close() {
		stop();
	}

	public void stop() {
		stop(ExecutorTerminator.DEFAULT_TASK_FINISH_PERIOD_MS, ExecutorTerminator.DEFAULT_TASK_STOP_PERIOD_MS);
	}

	public void stop(long taskFinishPeriodMs, long taskStopPeriodMs) {

		stop = true;
		// stop pollers
		cancelScheduled(failOverPoll);
		cancelScheduled(expiredPoll);
		cancelScheduled(heartBeatPoll);
		if (isShutdownExecutorOnClose() && getScheduledExecutorService() != null) {
			boolean closed = false;
			if (taskFinishPeriodMs < 0L || taskStopPeriodMs < 0L) {
				closed = ExecutorTerminator.closeSilent(getScheduledExecutorService());
			} else {
				closed = ExecutorTerminator.closeSilent(getScheduledExecutorService(), taskFinishPeriodMs, taskStopPeriodMs);
			}
			if (closed) {
				setScheduledExecutorService(null);
			} else {
				log.warn("Could not stop scheduled polling tasks executor.");
			}
		} 
	}
	
	protected void cancelScheduled(DbPollTask pt) {
		
		if (pt != null) {
			ScheduledFuture<Void> scheduledPoll = pt.getScheduledPoll();
			if (scheduledPoll != null) {
				scheduledPoll.cancel(true);
			}
		}
	}

	public ScheduledExecutorService createScheduledExecutorService(int coreThreads, int maxPoolSize) {

		ScheduledThreadPoolExecutor e = new ScheduledThreadPoolExecutor(coreThreads < 1 ? 1 : coreThreads,
				new DaemonThreadFactory(this.getClass().getSimpleName()));
		e.setKeepAliveTime(60L, TimeUnit.SECONDS);
		e.allowCoreThreadTimeOut(true);
		e.setRemoveOnCancelPolicy(true);
		e.setMaximumPoolSize(maxPoolSize < coreThreads ? coreThreads : maxPoolSize);
		return e;
	}
	
}

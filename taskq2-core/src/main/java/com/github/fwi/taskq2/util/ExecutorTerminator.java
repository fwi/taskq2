package com.github.fwi.taskq2.util;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExecutorTerminator {

	private static final Logger log = LoggerFactory.getLogger(ExecutorTerminator.class);

	public static final long DEFAULT_TASK_FINISH_PERIOD_MS = 5000L;
	public static final long DEFAULT_TASK_STOP_PERIOD_MS = 2000L;

	private ExecutorTerminator() {}

	public static boolean closeSilent(ExecutorService executor) {
		return closeSilent(executor, DEFAULT_TASK_FINISH_PERIOD_MS, DEFAULT_TASK_STOP_PERIOD_MS);
	}

	public static boolean closeSilent(ExecutorService executor, long taskFinishPeriodMs, long taskStopPeriodMs) {
		
		boolean closed = false;
		try {
			closed = close(executor, taskFinishPeriodMs, taskStopPeriodMs, false);
		} catch (InterruptedException e) {
			// won't happen
		}
		return closed;
	}

	public static boolean close(ExecutorService executor) throws InterruptedException {
		return close(executor, DEFAULT_TASK_FINISH_PERIOD_MS, DEFAULT_TASK_STOP_PERIOD_MS);
	}

	public static boolean close(ExecutorService executor, long taskFinishPeriodMs, long taskStopPeriodMs) throws InterruptedException {
		return close(executor, taskFinishPeriodMs, taskStopPeriodMs, true);
	}

	public static boolean close(ExecutorService executor, long taskFinishPeriodMs, long taskStopPeriodMs, boolean allowInterrupt) throws InterruptedException {

		executor.shutdown();
		boolean finished = executor.isTerminated();
		if (!finished && taskFinishPeriodMs > 0L) {
			try {
				finished = waitForTasks(executor, taskFinishPeriodMs);
			} catch (InterruptedException e) {
				if (allowInterrupt) {
					log.warn("Waiting for executor tasks to finish was interrupted.", e);
					throw e;
				}
			}
		}
		if (!finished) {
			log.debug("Interrupting running tasks.");
			executor.shutdownNow();
			if (taskStopPeriodMs > 0L) {
				try {
					finished = waitForTasks(executor, taskStopPeriodMs);
				} catch (InterruptedException e) {
					if (allowInterrupt) {
						log.warn("Waiting for interrupted executor tasks to stop was interrupted.", e);
						throw e;
					}
				}
			}
		}
		return finished;
	}

	public static boolean waitForTasks(ExecutorService executor, long waitTimeMs) throws InterruptedException {

		long sleepTimeMs = waitTimeMs / 10;
		if (sleepTimeMs < 1000L) {
			sleepTimeMs = 1000L;
			if (sleepTimeMs > waitTimeMs) {
				sleepTimeMs = waitTimeMs;
			}
		}
		long startTime = System.currentTimeMillis();
		while (startTime + waitTimeMs > System.currentTimeMillis()) {
			executor.awaitTermination(sleepTimeMs, TimeUnit.MILLISECONDS);
			if (executor.isTerminated()) {
				break;
			} else {
				log.debug("Waiting for running tasks to finish.");
			}
		}
		return executor.isTerminated();
	}

}

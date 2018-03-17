package com.github.fwi.taskq2;

import java.io.Closeable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.fwi.taskq2.util.BinarySemaphore;
import com.github.fwi.taskq2.util.DaemonThreadPool;
import com.github.fwi.taskq2.util.ExecutorTerminator;

public class TqGroup implements Closeable {

	private static final Logger log = LoggerFactory.getLogger(TqGroup.class);

	private ExecutorService executor;
	private boolean closeExecutor;
	private final ConcurrentHashMap<String, TqBase> taskQueues = new ConcurrentHashMap<>();
	private final BinarySemaphore tasksAvailable = new BinarySemaphore();
	private final BinarySemaphore pauseLock = new BinarySemaphore();
	private final AtomicLong tasksAdded = new AtomicLong(); 
	private final AtomicLong tasksDone = new AtomicLong();
	private final AtomicInteger tasksQueued = new AtomicInteger();
	private final CountDownLatch awaitTasksDone = new CountDownLatch(1);
	private volatile boolean awaitingTasksDone;
	private boolean stopping, started, paused;
	
	public boolean addTask(String qname, Object tdata) {
		return addTask(qname, tdata, null);
	}

	public boolean addTask(String qname, Object tdata, String qosKey) {
		return addTask(qname, new TqEntry(tdata, qosKey));
	}
	
	public boolean addTask(String qname, TqEntry te) {
		
		TqBase tq = taskQueues.get(qname); 
		if (tq == null) {
			return false;
		}
		tq.addTask(te);
		tasksAdded.incrementAndGet();
		tasksQueued.incrementAndGet();
		tasksAvailable.release();
		return true;
	}
	
	public void setExecutor(ExecutorService executor) { this.executor = executor; }
	public ExecutorService getExecutor() { return executor; }
	
	public void setCloseExecutor(boolean closeExecutor) { this.closeExecutor = closeExecutor; }
	public boolean isCloseExecutor() { return closeExecutor; }
	
	public void addQueue(TqBase tq) {
		taskQueues.put(tq.getName(), tq);
	}

	public TqBase getQueue(String qname) {
		return taskQueues.get(qname);
	}

	public TqBase removeQueue(String qname) {
		return taskQueues.remove(qname);
	}
	
	public synchronized void start() {
		
		if (started) {
			log.warn("Task queue group already started.");
			return;
		}
		started = true;
		starting();
		stopping = false;
	}
	
	protected void starting() {
		
		if (executor == null) {
			closeExecutor = true;
			executor = new DaemonThreadPool(this.getClass().getSimpleName());
		}
		executor.execute(new TaskExecLoop());
	}

	@Override
	public void close() {
		stop();
	}
	
	public void stop() {
		stop(ExecutorTerminator.DEFAULT_TASK_FINISH_PERIOD_MS, ExecutorTerminator.DEFAULT_TASK_STOP_PERIOD_MS);
	}

	public synchronized void stop(long taskFinishPeriodMs, long taskStopPeriodMs) {
		
		if (stopping) {
			log.debug("Task queue group already stopping.");
			return;
		}
		stopping = true;
		stopping(taskFinishPeriodMs, taskStopPeriodMs);
		started = false;
	}
	
	protected void stopping(long taskFinishPeriodMs, long taskStopPeriodMs) {
		
		pauseLock.release();
		tasksAvailable.release();
		if (closeExecutor) {
			if (ExecutorTerminator.closeSilent(executor, taskFinishPeriodMs, taskStopPeriodMs)) {
				log.debug("Task queue group executor stopped.");
				executor = null;
			} else {
				log.warn("Could not stop task executor.");
			}
		}
	}
	
	public synchronized boolean isPaused() {
		return paused;
	}
	
	/**
	 * (un)pauses the execution of tasks for all queues.
	 * <br>The pause-state of task queues themselves is unchanged
	 * (i.e. if given paused value is <tt>false</tt>, task queues that were paused remain paused).
	 * <br>See also {@link #setPaused(String, boolean)}.
	 */
	public synchronized void setPaused(boolean paused) {
		
		if (this.paused != paused) {
			this.paused = paused;
			if (!paused) {
				tasksAvailable.release();
				pauseLock.release();
			}
		}
	}

	/**
	 * (un)pauses the execution of tasks for one queue.
	 * @param qname the name of the queue to (un)pause.
	 * @return false if the task queue with the given name is not available. 
	 */
	public boolean setPaused(String qname, boolean paused) {
		
		TqBase tq = taskQueues.get(qname); 
		if (tq == null) {
			return false;
		}
		if (tq.isPaused() != paused) {
			tq.setPaused(paused);
			if (!paused) {
				tasksAvailable.release();
			}
		}
		return true;
	}
	
	/**
	 * Tasks in the queue might not get executed when, for example, a task-queue is paused 
	 * via {@link TqBase#setPaused(boolean)} instead of via
	 * this class' method {@link #setPaused(String, boolean)}.
	 * In such an unfortunate case, calling this method will trigger the execution of queued tasks.
	 */
	public void triggerTaskExec() {
		
		tasksAvailable.release();
	}

	protected void taskDone(TqBase tq, TqEntry te) {
		
		tq.taskDone(te);
		tasksDone.incrementAndGet();
		tasksQueued.decrementAndGet();
		if (awaitingTasksDone && tasksDone.get() >= tasksAdded.get()) {
			awaitTasksDone.countDown();
		} else {
			tasksAvailable.release();
		}
	}
	
	public int getSize() { return tasksQueued.get(); }
	
	public long getTasksAdded() { return tasksAdded.get(); }
	public long getTasksExecuted() { return tasksDone.get(); }
	
	public boolean awaitAllTasksDone(long timeout, TimeUnit tunit) throws InterruptedException {
		
		awaitingTasksDone = true;
		boolean allDone = (tasksDone.get() >= tasksAdded.get() ? true : awaitTasksDone.await(timeout, tunit));
		awaitingTasksDone = false;
		return allDone;
	}

	protected class TaskExecLoop implements Runnable {
		
		@Override
		public void run() {
			final String originalThreadName = updateThreadName();
			while (!stopping) {
				try { // catch any exception in the loop
					try { // catch interrupted exceptions thrown by locks
						if (paused) {
							pauseLock.acquire();
						}
						tasksAvailable.acquire();
					} catch (InterruptedException ie) {
						if (stopping) {
							log.debug("Stopping task execution loop after interrupt exception.");
						} else {
							log.warn("Interrupted while waiting for tasks to execute.", ie);
						}
						continue; // check on stopping, retry getting locks
					}
					for (TqBase tq : taskQueues.values()) {
						if (tq.isPaused()) {
							continue;
						}
						// execute tasks in queue
						int executed = 0;
						while (tq.getSize() > 0 && tq.getInProgress() < tq.getMaxConcurrent()) {
							TqEntry te = tq.getNextTask();
							if (te == null) {
								log.warn("Expected a task in queue " + tq.getName() + " but received no task data.");
								break;
							}
							ITaskHandler thandler = tq.getHandlerFactory().getTaskHandler(tq.getName());
							Runnable taskRun = new TaskHandlerRun(tq, thandler, te);
							executor.execute(taskRun);
							executed++;
							if (executed > tq.getMaxConcurrent()) {
								// give other queues a chance, we'll come back here
								if (tq.getSize() > 0) {
									tasksAvailable.release();
									if (log.isTraceEnabled()) {
										log.trace("Delaying additional tasks to execute for queue " + tq.getName() + " after handling other queues.");
									}
								}
								break; // handle tasks in next task queue
							}
						} // while tasks for this queue
						if (executed > 0 && log.isTraceEnabled()) {
							log.trace("Executed " + executed + " task(s) for queue " + tq.getName());
						}
					} // for task queues
				} catch (Exception e) {
					log.error("Failed to complete task execution loop.", e);
				}
			} // while !stopping.
			log.info("Task queue group executor loop stopped.");
			Thread.currentThread().setName(originalThreadName);
		}
		
		protected String updateThreadName() {
			
			final String orgTname = Thread.currentThread().getName();
			if (orgTname.contains("-thread-")) {
				int i = orgTname.indexOf("-thread-");
				String tname = orgTname.substring(0, i + 1) + TaskExecLoop.this.getClass().getSimpleName();
				Thread.currentThread().setName(tname);
			}
			return orgTname;
		}
	} // TaskExecLoop
	
	class TaskHandlerRun implements Runnable {
		
		private final TqBase tq;
		private final ITaskHandler thandler; 
		private final TqEntry te;
		
		public TaskHandlerRun(TqBase tq, ITaskHandler thandler, TqEntry te) {
			this.tq = tq;
			this.thandler = thandler;
			this.te = te;
		}
		
		@Override
		public void run() {
			
			try {
				thandler.onTask(te.getTaskData(), tq.getName(), te.getQosKey(), te.getTaskId());
			} finally {
				taskDone(tq, te);
			}
		}
	}
	
}

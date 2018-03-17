package com.github.fwi.taskq2.demo;

import com.github.fwi.taskq2.ITaskHandler;
import com.github.fwi.taskq2.TqEntry;
import com.github.fwi.taskq2.db.TqDbEntry;
import com.github.fwi.taskq2.db.TqDbGroup;

import nl.fw.util.jdbc.DbConnNamedStatement;

import static com.github.fwi.taskq2.demo.DemoQNames.*;

import java.security.MessageDigest;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * One instance of this class handles all tasks for all queues. 
 * Note that this means that this entire class needs to be thread-safe.
 * If you do not want to be thread-safe, create a TaskHandlerFactory
 * that creates a new TaskHandler for each new task that comes in
 * (see <tt>com.github.fwi.taskq2.TaskHandlerFactory</tt> for a template).
 * 
 * When handling tasks per queue is no longer trivial,
 * create separate classes for each queue.
 * 
 */
public class TasksHandler implements ITaskHandler {

	static Logger log = LoggerFactory.getLogger(Demo.class);

	DemoGroup dgroup;
	TqDbGroup tgroup;
	
	public void setDemoGroup(DemoGroup dgroup) {
		this.dgroup = dgroup;
		this.tgroup = dgroup.tgroup;
	}
	
	@Override
	public void onTask(Object tdata, String qname, String qosKey, long taskId) {

		try {
			switch (qname) {
			case TQ_GENERATOR: 
				generator(taskId);
				break;
			case TQ_ANALYZER:
				analyzer(tdata, taskId);
				break;
			case TQ_CONVERTER:
				converter(tdata, taskId);
				break;
			case TQ_STATS:
				stats(tdata, taskId);
				break;
			case TQ_ERROR:
				error(tdata, taskId);
				break;
			default:
				abandonTaskInUnknownQueue(qname, taskId);
				break;
			} // switch
		} catch (Exception e) {
			TaskDataError terror = new TaskDataError(taskId, e);
			try {
				TqEntry tentry = new TqEntry(terror);
				storeAndQueueNewTask(TQ_ERROR, tentry, 0L);
				log.debug("Stored new error task {} for task {}", tentry.getTaskId(), taskId);
			} catch (Exception e2) {
				log.error("Unable to store error task after exception: {}", e.toString(), e2);
				/*
				 * Not much else can be done at this stage.
				 * The dbPoller will eventually pickup the tasks that were left behind.
				 */
			}
		}
	}

	void abandonTaskInUnknownQueue(String qname, long taskId) throws Exception {

		try (DbConnNamedStatement<?> dbc = dgroup.getDbConn()) {
			tgroup.abandonTask(dbc, taskId);
			dbc.commitAndClose();
		}
		log.error("Abandoned task {} in unknown task-queue {}", taskId, qname);
	}

	/*
	 * Utility method: create a new task in a new queue,
	 * (optionally) delete the old task and enqueue the new task. 
	 */
	void storeAndQueueNewTask(String qname, TqEntry task, long oldTaskId) throws Exception {

		try (DbConnNamedStatement<?> dbc = dgroup.getDbConn()) {
			tgroup.storeTask(dbc, qname, task);
			if (oldTaskId > 0L) {
				/*
				 * In case of error- and retry-handling, we do not want to remove the old task.
				 * Instead, re-queue the old task.
				 */
				tgroup.deleteTask(dbc, oldTaskId);
			}
			dbc.commitAndClose();
		}
		tgroup.addTask(qname, task);
	}

	/*
	 * Byte-pattern used to identify the retry-task.
	 * The generator-queue creates this task once
	 * and the analyzer queue throws an exception when the pattern is found.
	 */
	final byte[] RETRY_ME = new byte[] { 1, 2, 3, 4};
	
	final AtomicBoolean retryTaskDone = new AtomicBoolean();
	
	void generator(long taskId) throws Exception {

		Random random = new Random();
		int size = 16 + random.nextInt(64 - 16);
		byte[] bytes = new byte[size];
		random.nextBytes(bytes);
		if (retryTaskDone.compareAndSet(false, true)) {
			size = RETRY_ME.length;
			bytes = RETRY_ME;
		}
		storeAndQueueNewTask(TQ_ANALYZER, new TqEntry(new TaskData(bytes)), taskId);
		log.debug("[{}] Generated {} bytes.", taskId, size);
	}

	void analyzer(Object tdata, long taskId) throws Exception {

		TaskData taskData = (TaskData) tdata;
		if (Arrays.equals(RETRY_ME, taskData.bytes)) {
			log.info("Analyzer throwing exception for RETRY_ME task.");
			throw new RuntimeException("RETRY_ME");
		}
		// MD5 digester is not thread-safe, so need to create a new one each time.
		MessageDigest md = MessageDigest.getInstance("MD5");
		byte[] md5 = md.digest(taskData.bytes);
		StringBuilder sb = new StringBuilder();
		// MD5-hex, see https://stackoverflow.com/a/6565597
		for (int i = 0; i < md5.length; ++i) {
			sb.append(Integer.toHexString((md5[i] & 0xFF) | 0x100).substring(1,3));
		}
		taskData.md5 = sb.toString();
		storeAndQueueNewTask(TQ_CONVERTER, new TqEntry(taskData, getQosKey(taskData)), taskId);
		log.debug("[{}] Analyzed MD5 {}.", taskId, taskData.md5);
	}

	String getQosKey(TaskData taskData) {
		return taskData.md5.substring(0, 1);
	}

	void converter(Object tdata, long taskId) throws Exception {

		TaskData taskData = (TaskData) tdata;
		// Base64 encoder is thread safe, no need to create instances.
		taskData.b64 = Base64.getEncoder().encodeToString(taskData.bytes);
		storeAndQueueNewTask(TQ_STATS, new TqEntry(taskData), taskId);
		log.debug("[{}] Converted Base64 {}.", taskId, taskData.b64);
	}

	/*
	 * Some variables to keep statistics.
	 */
	public final Map<String, Integer> md5Distrib  = new HashMap<>();
	public final AtomicInteger tasksCount = new AtomicInteger();
	public final AtomicLong b64Length = new AtomicLong();

	void stats(Object tdata, long taskId) throws Exception {

		TaskData taskData = (TaskData) tdata;
		synchronized(md5Distrib) {
			String qk = getQosKey(taskData);
			if (md5Distrib.containsKey(qk)) {
				md5Distrib.put(qk, md5Distrib.get(qk) + 1);
			} else {
				md5Distrib.put(qk, 1);
			}
		}
		int tcount = tasksCount.incrementAndGet();
		b64Length.addAndGet(taskData.b64.length());
		// Done, this is the last queue.
		try (DbConnNamedStatement<?> dbc = dgroup.getDbConn()) {
			tgroup.deleteTask(dbc, taskId);
			dbc.commitAndClose();
		}
		log.debug("[{}] Stats collected {}.", taskId, tcount);
		if (tcount % 2_500 == 0) {
			log.info("Stats collected for {} tasks.", tcount);
		}

	} // void stats

	int MAX_RETRIES = 2;

	/*
	 * Rudimentary error-handling.
	 * Retry a task up to MAX_RETRIES and then abandon it.
	 */
	void error(Object tdata, long taskId) throws Exception {

		TaskDataError terror = null;
		try {
			terror = (TaskDataError) tdata;
		} catch (Exception e) {
			log.error("[{}] Invalid error task data: ", taskId, tdata);
			try (DbConnNamedStatement<?> dbc = dgroup.getDbConn()) {
				tgroup.deleteTask(dbc, taskId);
				dbc.commitAndClose();
			}
			/*
			 * Not much else can be done here.
			 * If there is an original task, the dbPoller will eventually find and load it.
			 */
			return;
		}
		boolean abandoned = false;
		TqDbEntry dbtask = null;
		/*
		 * Update the retry-count of the original task "in error" and delete the task for this error-queue.
		 */
		try (DbConnNamedStatement<?> dbc = dgroup.getDbConn()) {
			dbtask = tgroup.updateTaskRetry(dbc, terror.errorTaskId, 1);
			if (dbtask.getRetryCount() > MAX_RETRIES) {
				tgroup.abandonTask(dbc, terror.errorTaskId);
				abandoned = true;
			}
			tgroup.deleteTask(dbc, taskId);
			dbc.commitAndClose();
		}
		if (abandoned) {
			log.error("Task {} abandoned after exception.", terror.errorTaskId, terror.e);
		} else {
			boolean added = tgroup.addTask(dbtask.getQname(), tgroup.toTqEntry(dbtask));
			log.info("Retrying task {} (retry {}) on queue {} {}after exception: {}",
						dbtask.getTaskId(), dbtask.getRetryCount(), dbtask.getQname(), 
						added ? "" : "(NOT enqueued) ", terror.e.toString());
		}
	} // void error

}

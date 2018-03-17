package com.github.fwi.taskq2.demo;

import static com.github.fwi.taskq2.demo.DemoQNames.*;

import java.io.Closeable;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.fwi.taskq2.SingletonTaskHandlerFactory;
import com.github.fwi.taskq2.TqEntry;
import com.github.fwi.taskq2.TqFifo;
import com.github.fwi.taskq2.TqQos;
import com.github.fwi.taskq2.db.TqDbConf;
import com.github.fwi.taskq2.db.TqDbGroup;
import com.github.fwi.taskq2.db.TqDbInit;
import com.github.fwi.taskq2.db.TqDbServer;
import com.github.fwi.taskq2.db.poll.TqDbPoll;
import com.github.fwi.taskq2.util.PrettyPrintMap;

import ch.qos.logback.classic.LoggerContext;
import nl.fw.util.jdbc.DbConnNamedStatement;
import nl.fw.util.jdbc.hikari.DbConnHik;
import nl.fw.util.jdbc.hikari.HikPool;

/**
 * Demo (runnable class) for TaskQ2.
 * 
 * In order to run this demo, first checkout and install the <tt>taskq2</tt> project:
 * <br>cd <tt>taskq2</tt>
 * <br>mvn clean install</tt>
 * <br>Then go to this project and run the demo via Maven:
 * <br><tt>cd taskq2-demo</tt>
 * <br><tt>mvn package exec:java</tt>
 * <p>
 * This Demo shows the steps required to setup TaskQ with a database and configure queues, 
 * including some rudimentary error handling.
 * <p>
 * For more or less detailed logging, open <tt>logback.xml</tt> in the main resources directory
 * and set category <tt>com.github.fwi.taskq2.demo</tt> to <tt>debug</tt> or <tt>info</tt>
 * 
 */
public class Demo implements Closeable {

	static Logger log = LoggerFactory.getLogger(Demo.class);

	/*
	 * There is a big difference in performance for this Demo
	 * between the default HSQL- versus PostgreSQL/MySQL-database (see also AMOUNT_OF_TASKS below).
	 * If you can, prepare a PostgreSQL (or MySQL) database as described in 
	 * taskq2-jdbc/src/main/resources/taskq-db-struct-pgsql.sql (or taskq-db-struct-mysql.sql).
	 * Update "src/main/resource/db-taskq-pg.properties" if needed.
	 */
	static boolean USE_POSTGRESQL = false;
	
	public static void main(String[] args) {

		if (USE_POSTGRESQL) {
			HikPool.DB_DEFAULTS_FILE = "db-taskq-defaults-pg.properties";
		} else {
			HikPool.DB_DEFAULTS_FILE = "db-taskq-defaults.properties";
		}
		// auto-closeable so no threads remain hanging.
		try (Demo demo = new Demo()) {
			demo.init();
			demo.addTasks();
			demo.await();
		} catch (Exception e) {
			log.error("Demo failed.", e);
		} finally {
			log.info("Done");
			// shutdown the logger-threads, else mvn exec:java cannot finish.
			LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();
			loggerContext.stop();
		}
	}

	/*
	 * This Demo has very intensive database usage and the default "in memory" database
	 * will eat about 3 GB for 20 000 records.
	 * PostgreSQL will be busy (high CPU usage) 
	 * but memory usage stays below 512MB for any amount of records. 
	 * 20 000 tasks should take about a minute to complete.
	 * 
	 * To see what and when the queues are doing work,
	 * set this number to 10 and enable debug logging as explained above. 
	 */
	final int AMOUNT_OF_TASKS = 10_000;
	
	/*
	 * How long to wait for processing of all tasks.
	 * All processing will stop after the specified seconds, even if there are still tasks in queues.
	 */
	final long MAX_WAIT_TIME_SECONDS = 60;

	/*
	 * In this demo, one class is used that handles messages for different queues.
	 */
	final TasksHandler tasksHandler = new TasksHandler();
	
	/*
	 * These are the only variables that need to be shared 
	 * in order to create tasks for new queues, do error handling, retry handling, etc..
	 */
	HikPool dbPool;
	TqDbGroup tgroup;
	DemoGroup dgroup;

	/*
	 * Setup all the components to get TaskQ2 going.
	 */
	void init() throws Exception {

		dbPool = new HikPool();
		TqDbConf dbConf = new TqDbConf();
		/*
		 * Settings below allow for maximum throughput, but these are not normal settings!
		 * E.g. an expire time of 3 seconds is too low: chances are high that a task-record
		 * is updated by the "load expired" poller while the queue is also updating the task-record.
		 * This can always happen but it is not desirable 
		 * and should be prevented with a higher expire time value.
		 */
		dbConf.setMaxSize(8_000);
		dbConf.setMaxSizePerQ(7_000);
		dbConf.setExpireTimeS(3);
		dbConf.setReloadIntervalMs(3_000L);
		dbConf.setDbReloadMinFree(10);
		dbConf.setDbReloadMaxAmount(dbConf.getMaxSizePerQ());
		dbConf.setDbReloadLogInterval(dbConf.getMaxSizePerQ() / 5);
		
		TqDbInit dbInit = new TqDbInit(dbConf);
		if (USE_POSTGRESQL) {
			dbConf.setNamedQueries(dbInit.loadNamedQueries("taskq-db-queries.sql", "taskq-db-queries-pgsql.sql"));
			dbConf.setDbStructResource("taskq-db-struct-pgsql.sql");
		}
		
		TqDbServer dbServer = new TqDbServer(dbConf);
		tgroup = new TqDbGroup(dbServer);
		if (USE_POSTGRESQL) {
			dbPool.open(dbPool.loadDbProps("db-taskq-pg.properties", "db.taskq."));
		} else {
			dbPool.open(dbPool.loadDbProps("db-taskq.properties", "db.taskq."));
		}
		/*
		 * The dbPoller needs a datasource which requires dbPool to be opened first.
		 */
		TqDbPoll dbPoller = new TqDbPoll(tgroup, dbPool.getDataSource());
		tgroup.setPoller(dbPoller);

		dgroup = new DemoGroup(dbPool, tgroup);
		tasksHandler.setDemoGroup(dgroup);
		/*
		 * One instance of one class handles all messages from all queues.
		 */
		SingletonTaskHandlerFactory tasksHandlerFactory = new SingletonTaskHandlerFactory(tasksHandler);
		tgroup.addQueue(new TqFifo(TQ_GENERATOR, tasksHandlerFactory));
		tgroup.addQueue(new TqFifo(TQ_ANALYZER, tasksHandlerFactory));
		/*
		 * A QosKey is not effectively used in this demo, but this is all that is required for setup.
		 */
		TqQos qosq = new TqQos(TQ_CONVERTER, tasksHandlerFactory);
		qosq.setMaxConcurrentPerQosKey(1);
		tgroup.addQueue(qosq);
		tgroup.addQueue(new TqFifo(TQ_STATS, tasksHandlerFactory));
		tgroup.addQueue(new TqFifo(TQ_ERROR, tasksHandlerFactory));
		tgroup.addQueue(new TqFifo(TQ_UNKNOWN, tasksHandlerFactory));

		/*
		 * Initialize the database with tables and register the one (and only) server.
		 * Server registration is required for the dbPoller to work.
		 */
		try (DbConnNamedStatement<?> dbc = new DbConnHik(dbPool)) {
			dbInit.initDb(dbc);
			dbServer.registerServer(dbc);
		}
		/*
		 * Create thread-pools, start the task-handler loop and start the dbPoller. 
		 */
		tgroup.start();
	}

	void addTasks() throws Exception {

		LinkedList<TqEntry> tasks = new LinkedList<>();
		// One task to show how "unknown queue name" can be handled. 
		TqEntry badQTask = new TqEntry("bad queue");
		log.info("Creating {} tasks in database.", AMOUNT_OF_TASKS);
		try (DbConnNamedStatement<?> dbc = dgroup.getDbConn()) {
			for (int i = 0; i < AMOUNT_OF_TASKS; i++) {
				TqEntry task = new TqEntry();
				/*
				 * This only stores tasks, they are not in a queue yet.
				 * If nothing is done, the dbPoller will eventually (default after 2 minutes) see "expired" records
				 * and add all records-tasks to queues.
				 * 
				 * TQ_GENERATOR does not expect any task-data, 
				 * so we can just create a task with no/null data.
				 */
				tgroup.storeTask(dbc, TQ_GENERATOR, task);
				tasks.add(task);
				if (i % 100 == 0) {
					// 100 inserts in one transaction is enough.
					dbc.commit();
					for (TqEntry t : tasks) {
						/*
						 * As soon as we add the task to a queue, tasks will be handled by queue-handlers.
						 */
						tgroup.addTask(TQ_GENERATOR, t);
					}
					tasks.clear();
				}
				if (i > 0 && i % 5_000 == 0) {
					log.info("Added {} tasks.", i);
				}
			}
			tgroup.storeTask(dbc, TQ_UNKNOWN, badQTask);
			dbc.commitAndClose();
		}
		tgroup.addTask(TQ_UNKNOWN, badQTask);
		for (TqEntry task : tasks) {
			/*
			 * We do not care if tasks are not actually added to the queue when the queue is full
			 * (in which case "addTask" will return false).
			 * The dbPoller will load any "expired" tasks from the database.  
			 */
			tgroup.addTask(TQ_GENERATOR, task);
		}
		log.info("Added {} tasks to generator queue.", AMOUNT_OF_TASKS);
	}
	
	void await() throws Exception {
		
		/*
		 * Some extra wait-logic added: if there are still tasks in the database, continue waiting.
		 * Tasks will be added from the database after the expire time.
		 */
		long tstart = System.currentTimeMillis();
		while (true) {
			Thread.sleep(100);
			long remaining = tstart - System.currentTimeMillis() + MAX_WAIT_TIME_SECONDS * 1000;
			if (remaining < 0) {
				break;
			}
			// wait for all tasks in memory in queues
			tgroup.awaitAllTasksDone(remaining, TimeUnit.SECONDS);
			// check for tasks waiting in the database
			if (tgroup.getPoller().getDao().getActiveTasksCount() < 1L) {
				break;
			}
		}
		printStats();
	}

	void printStats() {
		
		log.info("Tasks with stats: {}", tasksHandler.tasksCount.get());
		log.info("Average b64 length: {}", tasksHandler.b64Length.get() / tasksHandler.tasksCount.get());
		log.info("Tasks handler qos-key stats: {}", new PrettyPrintMap((Map<?,?>) tasksHandler.md5Distrib));
	}

	@Override
	public void close() {

		if (tgroup != null) {
			tgroup.close();
			tgroup = null;
		}
		if (dbPool != null) {
			dbPool.close();
			dbPool = null;
		}
	}

}

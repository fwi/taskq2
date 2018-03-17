package com.github.fwi.taskq2.demo;

/**
 * The queue names. Each queue sends messages to the next queue.
 * Each queue can use 4 workers by default, 
 * so we could end up with 5 * 4 = 20 active threads in the thread-pool of the taskq-group (TqGroup).
 */
public class DemoQNames {

	public static final String TQ_GENERATOR = "RandomTaskGenerator"; // first queue
	public static final String TQ_ANALYZER = "TaskDataAnalyzer";
	public static final String TQ_CONVERTER = "TaskDataConverter";
	public static final String TQ_STATS = "TaskStats"; // last queue
	public static final String TQ_ERROR = "TaskError"; // only used when exceptions happen
	public static final String TQ_UNKNOWN = "TaskQUnknown"; // just for demo - this queue is unknown to the tasks-handler.

}

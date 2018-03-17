package com.github.fwi.taskq2.db.poll;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.fwi.taskq2.ITaskHandlerFactory;
import com.github.fwi.taskq2.SingletonTaskHandlerFactory;
import com.github.fwi.taskq2.TaskHandler;
import com.github.fwi.taskq2.db.DbTestTask;

import nl.fw.util.jdbc.DbConn;
import nl.fw.util.jdbc.DbConnNamedStatement;

public class DbTestTaskHandler extends TaskHandler {

	private static final Logger log = LoggerFactory.getLogger(DbTestTaskHandler.class);

	private static final SingletonTaskHandlerFactory factory = new SingletonTaskHandlerFactory(new DbTestTaskHandler());
	
	public static ITaskHandlerFactory getFactory() {
		return factory;
	}
	
	public static void setFactoryDbConn(DbConnNamedStatement<?> c) {
		((DbTestTaskHandler)factory.getTaskHandler(null)).internalConn = c;
	}
	
	/** NOT thread-safe! */
	public DbConnNamedStatement<?> internalConn;
	
	@Override
	public void onTask(Object tdata) {
		
		DbTestTask t = (DbTestTask) tdata;
		if (t.tgroup == null) {
			return;
		}
		log.debug("Running {} - {} - {}", t.qname, t.id, t.item);
		@SuppressWarnings("resource")
		DbConnNamedStatement<?> c = new DbConn(internalConn.getDataSource()).setNamedQueries(internalConn.getNamedQueries());
		try {
			if ("q1".equals(t.qname)) {
				t.tgroup.updateTaskQname(c, "q2", t.id);
			} else {
				t.tgroup.deleteTask(c, t.id);
			}
			c.commitAndClose();
			if ("q1".equals(t.qname)) {
				t.tgroup.addTask("q2", t, t.id);
				log.debug("{} task updated.", t.id);
				t.updated.countDown();
			} else {
				log.debug("{} task deleted.", t.id);
				t.deleted.countDown();
			}
		} catch (Exception e) {
			c.rollbackAndClose(e);
		}
	}

}

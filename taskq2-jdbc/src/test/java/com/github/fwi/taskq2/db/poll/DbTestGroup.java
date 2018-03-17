package com.github.fwi.taskq2.db.poll;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.fwi.taskq2.TqEntry;
import com.github.fwi.taskq2.db.DbTestTask;
import com.github.fwi.taskq2.db.TqDbGroup;
import com.github.fwi.taskq2.db.TqDbServer;

public class DbTestGroup extends TqDbGroup {

	private static final Logger log = LoggerFactory.getLogger(TqDbGroup.class);

	public DbTestGroup(TqDbServer dbServer) {
		super(dbServer);
	}
	
	@Override
	public boolean addTask(String qname, TqEntry te) {
		
		DbTestTask tdata = (DbTestTask) te.getTaskData(); 
		tdata.qname = qname;
		tdata.tgroup = this;
		tdata.id = te.getTaskId();
		log.debug("[{}] Adding task data [{}]", qname, tdata.toString());
		return super.addTask(qname, te);
	}

}

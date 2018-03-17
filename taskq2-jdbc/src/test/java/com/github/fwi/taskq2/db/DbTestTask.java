package com.github.fwi.taskq2.db;

import java.io.IOException;
import java.io.Serializable;
import java.util.concurrent.CountDownLatch;

public class DbTestTask implements Serializable {

	private static final long serialVersionUID = 1L;
	
	public long id;
	public String item;
	public String qname;
	
	public transient CountDownLatch updated;
	public transient CountDownLatch deleted;

	public transient TqDbGroup tgroup;

	public DbTestTask() {
		this(null, null);
	}

	public DbTestTask(TqDbGroup tgroup, String qname) {
		super();
		this.tgroup = tgroup;
		this.qname = qname;
		this.item = this.hashCode() + " - " + this.getClass().toString();
		initTransient();
	}

	private void initTransient() {
		updated = new CountDownLatch(1);
		deleted = new CountDownLatch(1);
	}

	/**
	 * Called by ObjectInputStream, used to initialize transient variables.
	 * These are NOT initialized via the (default) constructor.
	 */
	private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException {
		
		in.defaultReadObject();
		initTransient();
	}

	@Override
	public String toString() {
		return item;
	}
}

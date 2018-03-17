package com.github.fwi.taskq2;

import java.util.concurrent.Semaphore;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.fwi.taskq2.util.SyncCountMap;
import com.github.fwi.taskq2.util.SyncListMap;

public class TqQos extends TqBase {

	private static final Logger log = LoggerFactory.getLogger(TqQos.class);

	private final SyncCountMap<String> inProgressPerKey = new SyncCountMap<>();
	private final SyncListMap<String, TqEntry> tasksPerKey = new SyncListMap<>();
	/** Need a fair semaphore so that add does not block poll forever and vice versa. */
	private final Semaphore updateLock = new Semaphore(1, true);

	private volatile int maxConcurrentPerQosKey;

	public TqQos() {
		this(null, null);
	}

	public TqQos(String name, ITaskHandlerFactory handlerFactory) {
		super(name, handlerFactory);
	}

	public int getMaxConcurrentPerQosKey() {
		return (maxConcurrentPerQosKey < 1 ? getMaxConcurrent() : maxConcurrentPerQosKey);
	}
	public void setMaxConcurrentPerQosKey(int maxConcurrentPerQosKey) {
		this.maxConcurrentPerQosKey = maxConcurrentPerQosKey;
	}

	@Override
	public void addTask(Object tdata) {
		addTask(tdata, null);
	}

	public void addTask(Object tdata, String qosKey) {
		addTask(new TqEntry(tdata, qosKey));
	}

	public void addTask(TqEntry te) {

		if (te.getQosKey() == null) {
			tasksPerKey.add(te.getQosKey(), te);
			return;
		}
		boolean havePermit = false;
		try {
			updateLock.acquire();
			havePermit = true;
			tasksPerKey.add(te.getQosKey(), te);
		} catch (Exception e) {
			log.error("Could not add task to queue " + getName() + ".", e);
		} finally {
			if (havePermit) {
				updateLock.release();
			}
		}
	}

	@Override
	public TqEntry getNextTask() {

		if (tasksPerKey.getSize() < 1) {
			return null;
		}
		boolean havePermit = false;
		TqEntry te = null;
		String qosKey = null;
		try {
			updateLock.acquire();
			havePermit = true;
			int maxKeys = tasksPerKey.getSizeKeys();
			if (maxKeys < 1) {
				// fast track - no need to check for any in-progress counts.
				qosKey = tasksPerKey.nextKey();
				te = tasksPerKey.remove(qosKey);
			} else {
				int maxPerKey = getMaxConcurrentPerQosKey();
				int keyNumber = 0;
				while (te == null && keyNumber < maxKeys) {
					String key = tasksPerKey.nextKey();
					if (inProgressPerKey.getCount(key) < maxPerKey) {
						qosKey = key;
						te = tasksPerKey.remove(key);
					}
					keyNumber++;
				}
				if (te == null) {
					qosKey = tasksPerKey.nextKey();
					te = tasksPerKey.remove(qosKey);
				}
			}
			if (te != null) {
				inProgress.incrementAndGet();
			}
		} catch (Exception e) {
			log.warn("Could not remove a task from queue " + getName() + ".", e);
		} finally {
			if (havePermit) {
				updateLock.release();
			}
		}
		return te;
	}
	
	@Override
	public void taskDone(TqEntry te) {
		
		super.taskDone(te);
		inProgressPerKey.decrement(te.getQosKey());
	}

	@Override
	public int getSize() {
		return tasksPerKey.getSize();
	}

	public int getSizeKeys() {
		return tasksPerKey.getSizeKeys();
	}

}

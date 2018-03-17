package com.github.fwi.taskq2.util;

import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A semaphore that releases at most one permit.
 * @author FWiers
 *
 */
public class BinarySemaphore extends Semaphore {

	private static final long serialVersionUID = -1478098294953714924L;
	
	private final AtomicBoolean havePermit = new AtomicBoolean();
	
	public BinarySemaphore() {
		this(false);
	}

	public BinarySemaphore(int permits) {
		this(permits, false);
	}

	public BinarySemaphore(boolean fair) {
		this(0, fair);
	}

	public BinarySemaphore(int permits, boolean fair) {
		super((permits > 0 ? 1 : 0), fair);
		havePermit.set(permits > 0);
	}

	@Override
	public void release() {
		
		if (havePermit.compareAndSet(false, true)) {
			super.release();
		}
	}
	
	@Override
	public boolean tryAcquire() {
		
		if (super.tryAcquire()) {
			havePermit.set(false);
			return true;
		}
		return false;
	}

	@Override
	public boolean tryAcquire(long timeout, TimeUnit tunit) throws InterruptedException {
		
		if (super.tryAcquire(timeout, tunit)) {
			havePermit.set(false);
			return true;
		}
		return false;
	}

	@Override
	public void acquire() throws InterruptedException {
		
		super.acquire();
		havePermit.set(false);
	}
	
	@Override
	public void acquireUninterruptibly() {
		
		super.acquireUninterruptibly();
		havePermit.set(false);
	}

	@Override
	public void acquireUninterruptibly(int permits) {
		
		for (int i = 0; i < permits; i++) {
			acquireUninterruptibly();
		}
	}

	@Override
	public int drainPermits() {
		return (tryAcquire() ? 1 : 0);
	}
}

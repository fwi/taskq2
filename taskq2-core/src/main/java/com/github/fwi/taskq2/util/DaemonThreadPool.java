package com.github.fwi.taskq2.util;

import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A {@link ThreadPoolExecutor} that uses daemon threads and has no queue.
 */
public class DaemonThreadPool extends ThreadPoolExecutor {
	
	public DaemonThreadPool() {
		this(null);
	}
	
	public DaemonThreadPool(String poolNamePrefix) {
        super(0, Integer.MAX_VALUE,
                60L, TimeUnit.SECONDS,
                new SynchronousQueue<Runnable>(), new DaemonThreadFactory(poolNamePrefix));
	}

	// Copied form Executors.DefaultThreadFactory
    public static class DaemonThreadFactory implements ThreadFactory {
        
    	private static final AtomicInteger poolNumber = new AtomicInteger();
        private final AtomicInteger threadNumber = new AtomicInteger();
        private final ThreadGroup group;
        private final String namePrefix;

        public DaemonThreadFactory() {
        	this(null);
        }
        
        public DaemonThreadFactory(String poolNamePrefix) {
            SecurityManager s = System.getSecurityManager();
            group = (s != null ? s.getThreadGroup() : Thread.currentThread().getThreadGroup());
            namePrefix = (poolNamePrefix == null ? "pool-" : poolNamePrefix + "-") 
            		+ poolNumber.incrementAndGet() + "-thread-";
        }

        @Override
        public Thread newThread(Runnable r) {
            
        	Thread t = new Thread(group, r, namePrefix + threadNumber.incrementAndGet(), 0);
            if (!t.isDaemon()) {
                t.setDaemon(true);
            }
            if (t.getPriority() != Thread.NORM_PRIORITY) {
                t.setPriority(Thread.NORM_PRIORITY);
            }
            return t;
        }
    }

}

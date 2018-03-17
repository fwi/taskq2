package com.github.fwi.taskq2.util;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class SyncCountMap<K> {
	
	private final Object mapLock = new Object();
	private final Map<K, Integer> map = new HashMap<K, Integer>();
	private final AtomicInteger noKey = new AtomicInteger();
	
	public int getCount(K key) {
		
		if (key == null) {
			return noKey.get();
		}
		Integer i = null;
		synchronized(mapLock) {
			i = map.get(key);
		}
		return (i == null ? 0 : i);
	}
	
	public int getSizeKeys() {
		
		int i = 0;
		synchronized(mapLock) {
			i = map.size();
		}
		return i;
	}
	
	public void increment(K key) {
		
		if (key == null) {
			noKey.incrementAndGet();
			return;
		}
		synchronized(mapLock) {
			Integer i = map.get(key);
			if (i == null) {
				map.put(key, 1);
			} else {
				map.put(key, i + 1);
			}
		}
	}
	
	public void decrement(K key) {
		
		if (key == null) {
			noKey.decrementAndGet();
			return;
		}
		synchronized(mapLock) {
			Integer i = map.get(key);
			if (i != null) {
				i = i - 1;
				if (i == 0) {
					map.remove(key);
				} else {
					map.put(key, i);
				}
			}
		}
	}

}

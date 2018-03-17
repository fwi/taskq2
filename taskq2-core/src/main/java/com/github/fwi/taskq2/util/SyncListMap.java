package com.github.fwi.taskq2.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SyncListMap<K, V> {
	
	private static final Logger log = LoggerFactory.getLogger(SyncListMap.class);

	private final ConcurrentLinkedQueue<V> noKey = new ConcurrentLinkedQueue<V>(); 
	private final AtomicInteger noKeySize = new AtomicInteger();
	private final Object mapLock = new Object();
	private final Map<K, List<V>> map = new HashMap<K, List<V>>();
	private final AtomicInteger size = new AtomicInteger();
	private final List<K> keyTurn = new ArrayList<K>();
	private volatile int keyTurnIndex = -1;
	
	public int getSize() {
		return size.get();
	}
	
	/**
	 * Number of non-null keys (null-key is not counted as part of keys-size).
	 */
	public int getSizeKeys() {
		
		int i = 0;
		synchronized(mapLock) {
			i = keyTurn.size();
		}
		return i;
	}

	public int getSize(K key) {
		
		if (key == null) {
			return noKeySize.get();
		}
		List<V> l = null;
		int count = 0;
		synchronized(mapLock) {
			l = map.get(key);
			count = (l == null ? 0 : l.size());
		}
		return count;
	}
	
	public boolean add(K key, V value) {
		
		if (value == null) {
			throw new IllegalArgumentException("Value for map may not be null.");
		}
		if (key == null) {
			noKey.add(value);
			noKeySize.incrementAndGet();
			size.incrementAndGet();
			return true;
		}
		synchronized(mapLock) {
			List<V> l = map.get(key);
			if (l == null) {
				l = new LinkedList<V>();
				map.put(key, l);
				keyTurn.add(key);
			}
			l.add(value);
			size.incrementAndGet();
		}
		return true;
	}
	
	public V remove(K key) {
		
		V value = null;
		if (key == null) {
			value = noKey.poll();
			if (value != null) {
				size.decrementAndGet();
				noKeySize.decrementAndGet();
			}
			return value;
		}
		synchronized(mapLock) {
			List<V> l = map.get(key);
			if (l != null) {
				value = l.remove(0);
				if (l.isEmpty()) {
					map.remove(key);
					int i = keyTurn.indexOf(key);
					keyTurn.remove(i);
					if (log.isTraceEnabled()) {
						log.trace("Removed Qos key [" + key + "] at index " + i + ", remaining Qos keys: " + keyTurn.size());
					}
					if (keyTurnIndex > i) {
						keyTurnIndex--;
					}
				}
			}
			if (value != null) {
				size.decrementAndGet();
			}
		}
		return value;
	}
	
	public K nextKey()  {
		
		K key = null;
		synchronized(mapLock) {
			if (keyTurn.isEmpty()) {
				return null;
			}
			if (keyTurnIndex >= keyTurn.size()) {
				keyTurnIndex = (noKeySize.get() > 0 ? -1 : 0);
			}
			if (keyTurnIndex > -1) {
				key = keyTurn.get(keyTurnIndex);
			}
			keyTurnIndex++;
		}
		if (log.isTraceEnabled()) {
			log.trace("Qos-key " + key);
		}
		return key;
	}

}

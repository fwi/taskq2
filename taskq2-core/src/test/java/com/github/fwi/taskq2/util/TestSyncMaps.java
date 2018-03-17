package com.github.fwi.taskq2.util;

import static org.junit.Assert.*;

import org.junit.Test;

import com.github.fwi.taskq2.util.SyncCountMap;
import com.github.fwi.taskq2.util.SyncListMap;

public class TestSyncMaps {

	@Test
	public void testCountMap() {
		
		SyncCountMap<String> cmap = new SyncCountMap<String>();
		cmap.increment(null);
		cmap.increment("one");
		cmap.increment("two");
		cmap.increment("two");
		cmap.increment("three");
		cmap.increment("three");
		cmap.increment("three");
		cmap.increment("three");
		cmap.decrement("three");
		assertEquals(1, cmap.getCount(null));
		assertEquals(1, cmap.getCount("one"));
		assertEquals(2, cmap.getCount("two"));
		assertEquals(3, cmap.getCount("three"));
		cmap.decrement(null);
		cmap.decrement("two");
		cmap.decrement("two");
		assertEquals(0, cmap.getCount(null));
		assertEquals(0, cmap.getCount("two"));
		assertEquals(2, cmap.getSizeKeys());
	}

	@Test
	public void testQosMap() {
		
		SyncListMap<Integer, String> qosMap = new SyncListMap<Integer, String>();
		// Null key-check
		
		qosMap.add(null, "test");
		assertEquals("test", qosMap.remove(qosMap.nextKey()));
		
		try {
			qosMap.add(null, null);
			fail("Null value should not be allowed.");
		} catch (IllegalArgumentException e) {}

		// Setup keys and values for removal from map.
		for (int i = 0; i < 9; i++) {
			Integer key = null;
			switch(i) {
			case 3: case 4: case 5: key = 1; break;
			case 6: case 7: case 8: key = 2; break;
			default: key = null;
			}
			qosMap.add(key, key + " / " + i);
		}
		// One extra for key 2.
		qosMap.add(2, 2 + " / " + 9);

		// Key turns should be null > 1 > 2 > null > 1 > 2 > etc.
		assertEquals(null, qosMap.nextKey());
		assertEquals(Integer.valueOf(1), qosMap.nextKey());
		assertEquals(Integer.valueOf(2), qosMap.nextKey());
		assertEquals(null, qosMap.nextKey());
		
		// Values should be FIFO per key.
		assertEquals(1 + " / " + 3, qosMap.remove(qosMap.nextKey()));
		assertEquals(2 + " / " + 6, qosMap.remove(qosMap.nextKey()));
		assertEquals(null + " / " + 0, qosMap.remove(qosMap.nextKey()));
		assertEquals(1 + " / " + 4, qosMap.remove(qosMap.nextKey()));
		assertEquals(2 + " / " + 7, qosMap.remove(qosMap.nextKey()));
		assertEquals(null + " / " + 1, qosMap.remove(qosMap.nextKey()));
		assertEquals(1 + " / " + 5, qosMap.remove(qosMap.nextKey()));
		assertEquals(2 + " / " + 8, qosMap.remove(qosMap.nextKey()));
		assertEquals(null + " / " + 2, qosMap.remove(qosMap.nextKey()));
		assertEquals(2 + " / " + 9, qosMap.remove(qosMap.nextKey()));
		
		// Map should be empty
		assertNull(qosMap.remove(qosMap.nextKey()));
		assertEquals(0, qosMap.getSize());
		assertEquals(0, qosMap.getSizeKeys());
		
	}
}

package com.github.fwi.taskq2.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class PrettyPrintMap {

	public static String NULL_MAP = "no map";
	public static String EMPTY_MAP = "empty map";
	private Map<?, ?> map;
	private String sep = "\n";
	private boolean useQuotedValues;
	private boolean useBracketValues;
	private String noValue = "null";

	public PrettyPrintMap(Map<?, ?> map) {
		this.map = map;
	}
	
	public PrettyPrintMap withEntrySeparator(String sep) {
		this.sep = sep;
		return this;
	}

	public PrettyPrintMap withNoValue(String noValue) {
		this.noValue = noValue;
		return this;
	}

	public PrettyPrintMap withQuotedValues() {
		useQuotedValues = true;
		return this;
	}
	
	public PrettyPrintMap withBracketValues() {
		useBracketValues = true;
		return this;
	}

	@Override
	public String toString() {
		
		if (map == null) {
			return NULL_MAP;
		}
		StringBuilder sb = new StringBuilder();
		if (map.isEmpty()) {
			return EMPTY_MAP;
		}
		List<String> keyList = new ArrayList<String>();
		Map<String, Object> keyMap = new HashMap<String, Object>();
		Iterator<?> iter = map.keySet().iterator(); 
        while (iter.hasNext()) {
        	Object v = iter.next();
        	String k = (v == null ? noValue : v.toString());
        	keyList.add(k);
        	keyMap.put(k, v);
        }
        Collections.sort(keyList);
		Iterator<String> kiter = keyList.iterator(); 
        while (kiter.hasNext()) {
        	String k = kiter.next();
        	sb.append('\n').append(k).append("=");
            if (useQuotedValues) {
            	sb.append('\"');
            } else if (useBracketValues) {
            	sb.append('[');
            }
            Object v = map.get(keyMap.get(k));
            sb.append(v == null ? noValue : v.toString());
            if (useQuotedValues) {
            	sb.append('\"');
            } else if (useBracketValues) {
            	sb.append(']');
            }
            if (iter.hasNext()) {
                sb.append(sep);
            }
        }
		return sb.toString();
	}

}

package com.aerospike.examples.ldt;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Value;
import com.aerospike.client.large.LargeList;

public class Utils {
	public static final String LDT_KEY = "key";
	public static final String LDT_VALUE = "value";
	public static final String LDT_TOP = "ldt-top";
	public static final String LDT_TAIL = "ldt-tail";
	
	public static Map<String, Object> makeValueMap(long key, Object value){
		Map<String, Object> map = new HashMap<String, Object>();
		map.put(LDT_KEY, key);
		map.put(LDT_VALUE, value);
		return map;
	}
	
	public static Map<String, Object> makeValueMap(Object key, Object value){
		Map<String, Object> map = new HashMap<String, Object>();
		map.put(LDT_KEY, key);
		map.put(LDT_VALUE, value);
		return map;
	}

	public static Map<String, Long> makeKeyMap(long key){
		Map<String, Long> map = new HashMap<String, Long>();
		map.put(LDT_KEY, key);
		return map;
	}
	public static Map<String, Object> makeKeyMap(Object key){
		Map<String, Object> map = new HashMap<String, Object>();
		map.put(LDT_KEY, key);
		return map;
	}

	public static Value makeValue(Object key, Object value){
		return Value.getAsMap(makeValueMap(key, value));
	}

	public static Value makeKeyAsValue(long key){
		return Value.getAsMap(makeKeyMap(key));
	}

	public static Value makeKeyAsValue(Object key) {
		return Value.getAsMap(makeKeyMap(key));
	}

	public static Object findElement(LargeList llist, Value key){
		List<Map<String, Object>> list = (List<Map<String, Object>>) llist.find(key);
		Object element = null;
		if (list != null && list.size() > 0)
			element = ((Map<String, ?>)list.get(0)).get(Utils.LDT_VALUE);
		return element;

	}
	
	public static int size(LargeList llist) {
		int size = 0;
		try {
			size = llist.size();
		} catch (AerospikeException e){
			if (e.getResultCode() != 1417) // LDT-Bin Does Not Exist
				throw e;
		}
		return size;
	}


}

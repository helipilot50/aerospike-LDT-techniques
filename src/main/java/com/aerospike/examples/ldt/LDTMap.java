package com.aerospike.examples.ldt;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.Value;
import com.aerospike.client.large.LargeList;


/**
@author Peter Milne
*/
public class LDTMap<K,V> implements Map<K,V>{
	private static final String LDT_KEY = "key";
	private static final String LDT_VALUE = "value";
	private AerospikeClient client;
	private Key key;
	LargeList llist;
	private String binName;

	public LDTMap(AerospikeClient client, Key key, String binName) throws AerospikeException {
		this.client = client;
		this.key = key;
		this.binName = binName;
	}
	
	private LargeList getList(){
		if (llist == null)
			llist = this.client.getLargeList(null, key, binName);
		return llist;
	}
	private Map<String, Object> makeKeyMap(Object key){
		Map<String, Object> map = new HashMap<String, Object>();
		map.put(LDT_KEY, key);
		return map;
	}
	
	private Map<String, Object> makeValueMap(Object key, Object value){
		Map<String, Object> map = new HashMap<String, Object>();
		map.put(LDT_KEY, key);
		map.put(LDT_VALUE, value);
		return map;
	}
	private Value makeValue(Object key, Object value){
		return Value.get(makeValueMap(key, value));
	}
	
	@Override
	public int size() {
		int size = 0;
		try {
			size = getList().size();
		} catch (AerospikeException e){
			if (e.getResultCode() != 1417) // LDT-Bin Does Not Exist
				throw e;
		}
		return size;
	}
	
	@Override
	public boolean isEmpty() {
		return size() == 0;
	}
	
	@Override
	public boolean containsKey(Object key) {
		return getList().find(Value.get(makeKeyMap(key))) != null;
	}
	
	@Override
	public boolean containsValue(Object value) {
		
		//TODO
		return false;
	}
	@SuppressWarnings("unchecked")
	@Override
	public V get(Object key) {
		List<Map<String, Object>> list = (List<Map<String, Object>>) getList().find(Value.get(makeKeyMap(key)));
		Object element = null;
		if (list != null && list.size() > 0)
			element = ((Map<String, ?>)list.get(0)).get(LDT_VALUE);
		return (V) element;
	}
	
	@Override
	public V put(Object key, Object value) {
		getList().update(makeValue(key, value));
		return null;
	}
	
	@Override
	public V remove(Object key) {
		@SuppressWarnings("unchecked")
		V value = (V) getList().find(Value.get(key));
		getList().remove(Value.get(key));
		return value;
	}
	
	@Override
	public void clear() {
		getList().destroy();
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public Set<K> keySet() {
		/*
		 * Large result will blow up your heap space
		 */
		Set<K> ks = new HashSet<K>();
		List<Map.Entry<K, V>> result = (List<java.util.Map.Entry<K, V>>) getList().scan();
        for (Map.Entry<? extends K, ? extends V> e : result) {
            K key = e.getKey();
            ks.add(key);
        }
        return ks;
	}

	@Override
	public void putAll(Map<? extends K, ? extends V> m) {
        for (Map.Entry<? extends K, ? extends V> e : m.entrySet()) {
            K key = e.getKey();
            V value = e.getValue();
            put(key, value);
        }
	}

	@SuppressWarnings("unchecked")
	@Override
	public Collection<V> values() {
		/*
		 * Large result will blow up your heap space
		 */
		List<Map<K, V>> result = (List<Map<K, V>>) getList().scan();
		List<V> values = new ArrayList<V>();
		for (Map<K, V> e : result) {
			V value = e.get(LDT_VALUE);
			values.add(value);
		}
		result.clear();
		return values;
	}

	@SuppressWarnings("unchecked")
	@Override
	public Set<Map.Entry<K, V>> entrySet() {
		/*
		 * Large result will blow up your heap space
		 */
		List<Map.Entry<K, V>> result = (List<java.util.Map.Entry<K, V>>) getList().scan();
		return new HashSet<Map.Entry<K, V>>(result);
	}

}
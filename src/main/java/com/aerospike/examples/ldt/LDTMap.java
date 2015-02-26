package com.aerospike.examples.ldt;

import java.util.ArrayList;
import java.util.Collection;
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
			llist = this.client.getLargeList(null, key, binName, null);
		return llist;
	}
	
	@Override
	public int size() {
		return Utils.size(getList());
	}
	
	@Override
	public boolean isEmpty() {
		return getList().size() == 0;
	}
	
	@Override
	public boolean containsKey(Object key) {
		return getList().find(Utils.makeKeyAsValue(key)) != null;
	}
	
	@Override
	public boolean containsValue(Object value) {
		
		//TODO
		return false;
	}
	@SuppressWarnings("unchecked")
	@Override
	public V get(Object key) {
		return (V) Utils.findElement(getList(), Utils.makeKeyAsValue(key));
	}
	
	@Override
	public V put(Object key, Object value) {
		getList().update(Utils.makeValue(key, value));
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
			V value = e.get(Utils.LDT_VALUE);
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
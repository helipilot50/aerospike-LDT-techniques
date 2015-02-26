package com.aerospike.examples.ldt;

import java.util.Collection;
import java.util.HashMap;
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
	
	
	/**
	 * the current size of the Large Map
	 */
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
	@Override
	public Set<K> keySet() {
		// TODO Auto-generated method stub
		return null;
	}


	@Override
	public void putAll(Map m) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Collection values() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Set entrySet() {
		// TODO Auto-generated method stub
		return null;
	}

}
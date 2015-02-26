package com.aerospike.examples.ldt;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

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

	private static Logger log = Logger.getLogger(LDTMap.class);
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
	
	private Map<Object, Object> makeMap(Object key, Object value){
		Map<Object, Object> map = new HashMap<Object, Object>();
		map.put(LDT_KEY, key);
		map.put(LDT_VALUE, value);
		return map;
	}
	
	/**
	 * the current size of the Large Map
	 */
	@Override
	public int size() {
		return getList().size();
	}
	@Override
	public boolean isEmpty() {
		return getList().size() == 0;
	}
	@Override
	public boolean containsKey(Object key) {
		// TODO Auto-generated method stub
		return false;
	}
	@Override
	public boolean containsValue(Object value) {
		// TODO Auto-generated method stub
		return false;
	}
	@Override
	public V get(Object key) {
		Map map = (Map) getList().find(Value.get(key));
		if (map != null)
			return (V) map.get(LDT_VALUE);
		else 
			return null;
	}
	@Override
	public V put(Object key, Object value) {
		getList().add(Value.getAsMap(makeMap(key, value)));
		return null;
	}
	@Override
	public V remove(Object key) {
		getList().remove(Value.get(key));
		return null;
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
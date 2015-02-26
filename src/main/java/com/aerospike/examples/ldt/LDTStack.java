package com.aerospike.examples.ldt;

import java.util.HashMap;
import java.util.Map;
import java.util.Stack;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.Value;
import com.aerospike.client.large.LargeList;

public class LDTStack<E> extends Stack<E> {
	private static final long serialVersionUID = -6871389568666262894L;
	private static final String LDT_KEY = "key";
	private static final String LDT_VALUE = "value";
	private AerospikeClient client;
	private Key key;
	LargeList llist;
	private String binName;

	public LDTStack(AerospikeClient client, Key key, String binName) throws AerospikeException {
		this.client = client;
		this.key = key;
		this.binName = binName;
	}
	
	private LargeList getList(){
		if (llist == null)
			llist = this.client.getLargeList(null, key, binName, null);
		return llist;
	}
	
	private Map<String, Object> makeMap(int key, E value){
		Map<String, Object> map = new HashMap<String, Object>();
		map.put(LDT_KEY, key);
		map.put(LDT_VALUE, value);
		return map;
	}

	@Override
	public E push(E item) {
		int top = getList().size();
		top++;
		getList().add(Value.getAsMap(makeMap(top, item)));
		return item;
	}
	@SuppressWarnings("unchecked")
	@Override
	public synchronized E pop() {
		int top = getList().size();
		E topElement = (E) getList().find(Value.get(top));
		getList().remove(Value.get(top));
		return topElement;
	}
	@Override
	public synchronized int size() {
		return getList().size();
	}
	@Override
	public void clear() {
		getList().destroy();
	}
	@SuppressWarnings("unchecked")
	@Override
	public synchronized E peek() {
		int top = getList().size();
		E topElement = (E) getList().find(Value.get(top));
		return topElement;
	}
	@Override
	public synchronized int search(Object o) {
		// TODO Auto-generated method stub
		return -1;
	}
}

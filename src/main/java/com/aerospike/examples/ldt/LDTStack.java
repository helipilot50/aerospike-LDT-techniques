package com.aerospike.examples.ldt;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Stack;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.Record;
import com.aerospike.client.Value;
import com.aerospike.client.large.LargeList;

public class LDTStack<E> extends Stack<E> {
	private static final long serialVersionUID = -6871389568666262894L;
	private static final String LDT_KEY = "key";
	private static final String LDT_VALUE = "value";
	private static final String STACK_TOP = "stack-top";
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
	
	private Map<String, Object> makeMap(long key, E value){
		Map<String, Object> map = new HashMap<String, Object>();
		map.put(LDT_KEY, key);
		map.put(LDT_VALUE, value);
		return map;
	}

	private long getTop(){
		long top = 0;
		try {
			Record record = this.client.get(null, this.key, STACK_TOP);
			top = record.getLong(STACK_TOP);
		} catch (AerospikeException e){

		}
		return top;
	}
	
	private long getNextTop(){
		Record record = this.client.operate(null, this.key, 
				Operation.add(new Bin(STACK_TOP, 1)),
				Operation.get(STACK_TOP));
		return record.getLong(STACK_TOP);
	}

	@Override
	public synchronized E push(E item) {
		long top = getNextTop();
		Map<String, Object> map = makeMap(top, item);
		getList().add(Value.getAsMap(map));
		return item;
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public synchronized E pop() {
		long top = getTop();
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
		long top = getTop();
		List<E> list = (List<E>) getList().find(Value.get(top));
		E topElement = list.get(0);
		return topElement;
	}
	
	@Override
	public synchronized int search(Object o) {
		// TODO Auto-generated method stub
		return -1;
	}
}

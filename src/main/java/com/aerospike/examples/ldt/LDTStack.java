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
import com.aerospike.client.ResultCode;
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

	private Map<String, Object> makeValueMap(long key, E value){
		Map<String, Object> map = new HashMap<String, Object>();
		map.put(LDT_KEY, key);
		map.put(LDT_VALUE, value);
		return map;
	}

	private Map<String, Long> makeKeyMap(long key){
		Map<String, Long> map = new HashMap<String, Long>();
		map.put(LDT_KEY, key);
		return map;
	}

	private Value makeKeyAsValue(long key){
		return Value.getAsMap(makeKeyMap(key));
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
		Map<String, Object> map = makeValueMap(top, item);
		getList().add(Value.getAsMap(map));
		return item;
	}

	@Override
	public synchronized E pop() {
		Value top = makeKeyAsValue(getTop());
		E topElement = findElement(top);
		getList().remove(top);
		return topElement;
	}

	@Override
	public synchronized int size() {
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
	public void clear() {
		getList().destroy();
		getList();
	}

	@Override
	public synchronized E peek() {
		long top = getTop();
		return findElement(top);
	}

	private E findElement(long key){
		return findElement(makeKeyAsValue(key));

	}
	@SuppressWarnings("unchecked")
	private E findElement(Value key){
		List<Map<String, ?>> list = (List<Map<String, ?>>) getList().find(key);
		E element = (E) ((Map<String, ?>)list.get(0)).get(LDT_VALUE);
		return element;

	}

	@Override
	public synchronized int search(Object o) {
		// TODO Auto-generated method stub
		return -1;
	}
}

package com.aerospike.examples.ldt;

import java.util.Stack;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.Value;
import com.aerospike.client.large.LargeList;

public class LDTStack<E> extends Stack<E> {
	private static final String STACK_MODULE = "stack";
	private static final long serialVersionUID = -6871389568666262894L;
	private AerospikeClient client;
	private Key key;
	LargeList llist;
	private String binName;

	public LDTStack(AerospikeClient client, Key key, String binName) throws AerospikeException {
		this.client = client;
		this.key = key;
		this.binName = binName;
	}

//	private LargeList getList(){
//		if (llist == null)
//			llist = this.client.getLargeList(null, key, binName, null);
//		return llist;
//	}


//	private long getTop(){
//		long top = 0;
//		try {
//			Record record = this.client.get(null, this.key, Utils.LDT_TOP);
//			top = record.getLong(Utils.LDT_TOP);
//		} catch (AerospikeException e){
//
//		}
//		return top;
//	}
//
//	private long getNextTop(){
//		Record record = this.client.operate(null, this.key, 
//				Operation.add(new Bin(Utils.LDT_TOP, 1)),
//				Operation.get(Utils.LDT_TOP));
//		return record.getLong(Utils.LDT_TOP);
//	}

	@Override
	public synchronized E push(E item) {
//		long top = getNextTop();
//		Map<String, Object> map = Utils.makeValueMap(top, item);
//		getList().add(Value.getAsMap(map));
		client.execute(null, this.key, STACK_MODULE, "push", Value.get(this.binName), Value.get(item));
		return item;
	}

	@Override
	public synchronized E pop() {
//		Value top = Utils.makeKeyAsValue(getTop());
//		@SuppressWarnings("unchecked")
//		E topElement = (E) Utils.findElement(getList(), top);
//		getList().remove(top);
		E topElement = (E) client.execute(null, this.key, STACK_MODULE, "pop", Value.get(this.binName));
		return topElement;
	}

	@Override
	public synchronized int size() {
		Long listSize = (long) client.execute(null, this.key, STACK_MODULE, "size", Value.get(this.binName));
		return listSize.intValue();
	}

	@Override
	public void clear() {
		client.execute(null, this.key, STACK_MODULE, "clear", Value.get(this.binName));
	}

	@Override
	public synchronized E peek() {
//		long top = getTop();
//		return findElement(top);
		E topElement = (E) client.execute(null, this.key, STACK_MODULE, "peek", Value.get(this.binName));
		return topElement;
	}

//	@SuppressWarnings("unchecked")
//	private E findElement(long key){
//		return (E) Utils.findElement(getList(), Utils.makeKeyAsValue(key));
//
//	}

	@Override
	public synchronized int search(Object o) {
		// TODO Auto-generated method stub
		return -1;
	}
}

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


	@Override
	public synchronized E push(E item) {
		client.execute(null, this.key, STACK_MODULE, "push", Value.get(this.binName), Value.get(item));
		return item;
	}

	@SuppressWarnings("unchecked")
	@Override
	public synchronized E pop() {
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

	@SuppressWarnings("unchecked")
	@Override
	public synchronized E peek() {
		E topElement = (E) client.execute(null, this.key, STACK_MODULE, "peek", Value.get(this.binName));
		return topElement;
	}


	@Override
	public synchronized int search(Object o) {
		// TODO Auto-generated method stub
		return -1;
	}
}

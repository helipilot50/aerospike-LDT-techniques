package com.aerospike.examples.ldt;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Queue;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.large.LargeList;

public class LDTQueue<E> implements Queue<E>{

	private static final String LDT_KEY = "key";
	private static final String LDT_VALUE = "value";
	private static final String QUEUE_TOP = "queue-top";
	private static final String QUEUE_TAIL = "queue-tail";
	private AerospikeClient client;
	private Key key;
	LargeList llist;
	private String binName;

	public LDTQueue(AerospikeClient client, Key key, String binName)  {
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
	
	private long getTop(){
		long top = 0;
		Record record = this.client.get(null, this.key, QUEUE_TOP);
		if (record != null){
			top = record.getLong(QUEUE_TOP);
		}
		return top;
	}

	private long getTail(){
		long tail = 0;
		Record record = this.client.get(null, this.key, QUEUE_TAIL);
		if (record != null){
			tail = record.getLong(QUEUE_TOP);
		}
		return tail;
	}

	@Override
	public int size() {
		return getList().size();
	}

	@Override
	public boolean isEmpty() {
		return (getList().size() == 0);
	}

	@Override
	public boolean contains(Object o) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public Iterator<E> iterator() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Object[] toArray() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <T> T[] toArray(T[] a) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean remove(Object o) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean containsAll(Collection<?> c) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean addAll(Collection<? extends E> c) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean removeAll(Collection<?> c) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean retainAll(Collection<?> c) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void clear() {
		getList().destroy();
	}

	@Override
	public boolean add(E e) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean offer(E e) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public E remove() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public E poll() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public E element() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public E peek() {
		// TODO Auto-generated method stub
		return null;
	}

}

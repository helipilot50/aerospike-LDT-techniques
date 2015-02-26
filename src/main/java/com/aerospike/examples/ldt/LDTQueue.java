package com.aerospike.examples.ldt;

import java.util.Collection;
import java.util.Iterator;
import java.util.Queue;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.Record;
import com.aerospike.client.Value;
import com.aerospike.client.large.LargeList;

public class LDTQueue<E> implements Queue<E>{

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
		if (llist == null){
			llist = this.client.getLargeList(null, key, binName, null);
			this.client.put(null, this.key, new Bin(Utils.LDT_TOP, 1));
		}
		return llist;
	}
	
	
	private long getTail(){
		long tail = 0;
		Record record = this.client.get(null, this.key, Utils.LDT_TAIL);
		if (record != null){
			tail = record.getLong(Utils.LDT_TAIL);
		}
		return tail;
	}
	private long getNextTail(){
		Record record = this.client.operate(null, this.key, 
				Operation.add(new Bin(Utils.LDT_TAIL, 1)),
				Operation.get(Utils.LDT_TAIL));
		return record.getLong(Utils.LDT_TAIL);
	}

	private long getTop(){
		long top = 0;
		Record record = this.client.get(null, this.key, Utils.LDT_TOP);
		if (record != null){
			top = record.getLong(Utils.LDT_TOP);
		}
		return top;
	}

	@Override
	public int size() {
		return Utils.size(getList());
	}

	@Override
	public boolean isEmpty() {
		return (size() == 0);
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
		long tail = getNextTail();
		getList().add(Utils.makeValue(tail, e));
		return false;
	}

	@Override
	public boolean offer(E e) {
		// TODO Auto-generated method stub
		return false;
	}

	@SuppressWarnings("unchecked")
	@Override
	public E remove() {
		long top = getTop();
		if (top == 0)
			return null;
		Value topValue = Utils.makeKeyAsValue(top);
		E tailElement = (E) Utils.findElement(getList(), topValue);
		getList().remove(topValue);
		this.client.add(null, this.key, new Bin(Utils.LDT_TOP, 1));
		return tailElement;
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

/* 
 * Copyright 2012-2015 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.aerospike.examples.ldt;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Queue;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Value;
import com.aerospike.client.large.LargeList;

public class LDTQueue<E> implements Queue<E>{

	private static final String QUEUE_MODULE = "queue";
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
		}
		return llist;
	}
	
	@Override
	public int size() {
		Long listSize = (long) client.execute(null, this.key, QUEUE_MODULE, "size", Value.get(this.binName));
		return listSize.intValue();
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
		List<Object> scanResilt = (List<Object>) getList().scan();
		return scanResilt.toArray();
	}

	@Override
	public <T> T[] toArray(T[] a) {
		List<Object> scanResilt = (List<Object>) getList().scan();
		return scanResilt.toArray(a);
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
		client.execute(null, this.key, QUEUE_MODULE, "clear", Value.get(this.binName));
	}

	@Override
	public boolean add(E e) {
		client.execute(null, this.key, QUEUE_MODULE, "add", Value.get(this.binName), Value.get(e));
		return true;
	}

	@Override
	public boolean offer(E e) {
		return add(e);
	}

	@Override
	public E remove() {
		E topElement = (E) client.execute(null, this.key, QUEUE_MODULE, "remove", Value.get(this.binName));
		if (topElement == null)
			throw new NoSuchElementException();
		return topElement;
	}

	@Override
	public E poll() {
		E topElement = (E) client.execute(null, this.key, QUEUE_MODULE, "remove", Value.get(this.binName));
		return topElement;
	}
	

	@Override
	public E element() {
		E topElement = (E) client.execute(null, this.key, QUEUE_MODULE, "peek", Value.get(this.binName));
		if (topElement == null)
			throw new NoSuchElementException();
		return topElement;
	}

	@Override
	public E peek() {
		E topElement = (E) client.execute(null, this.key, QUEUE_MODULE, "peek", Value.get(this.binName));
		return topElement;

	}

}

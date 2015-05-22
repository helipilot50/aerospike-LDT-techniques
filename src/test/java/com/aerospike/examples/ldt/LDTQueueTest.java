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

import java.util.List;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;

import junit.framework.Assert;

import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Value;

public class LDTQueueTest {
	public static final Logger LOG = Logger.getLogger(LDTQueueTest.class);
	AerospikeClient client;
	Key key;
	LDTQueue<String> subject;

	@Before
	public void setUp() throws Exception {
		client = new AerospikeClient("localhost", 3000);
		key = new Key("test", "demo", "the-queue-001");
		client.delete(null, key);
		subject = new LDTQueue<String>(client, key, "the-queue");
	}

	@After
	public void tearDown() throws Exception {
		client.close();
	}
	

	@Test
	public void testAdd() throws Exception {
		subject.add("cows");
		subject.add("sheep");
		subject.add("ducks");
		subject.add("mice");
		subject.add("dogs");
		subject.add("cats");
		subject.add("birds");
		int size = subject.size();
		Assert.assertEquals(7, size);
	}
	@Test
	public void testEmpty() throws Exception {
		subject.add("cows");
		subject.add("sheep");
		subject.add("ducks");
		subject.add("mice");
		subject.add("dogs");
		subject.add("cats");
		subject.add("birds");
		subject.clear();
		boolean nothing = subject.isEmpty();
		Assert.assertTrue(nothing);
		subject.add("birds");
		nothing = subject.isEmpty();
		Assert.assertFalse(nothing);
	}
	@Test
	public void testClear() throws Exception {
		subject.add("cows");
		subject.add("sheep");
		subject.add("ducks");
		subject.add("mice");
		subject.add("dogs");
		subject.add("cats");
		subject.add("birds");
		subject.clear();
		int size = subject.size();
		Assert.assertEquals(0, size);
		subject.add("birds");
		size = subject.size();
		Assert.assertEquals(1, size);
	}
	@Test(expected=NoSuchElementException.class)
	public void testRemove() throws Exception {
		subject.add("cows");
		subject.add("sheep");
		subject.add("ducks");
		subject.add("mice");
		subject.add("dogs");
		subject.add("cats");
		subject.add("birds");
		int size = subject.size();
		Assert.assertEquals(7, size);
		String animal = subject.remove();
		Assert.assertEquals("cows", animal);
		size = subject.size();
		Assert.assertEquals(6, size);
		animal = subject.remove();
		Assert.assertEquals("sheep", animal);
		size = subject.size();
		Assert.assertEquals(5, size);
		animal = subject.remove();
		Assert.assertEquals("ducks", animal);
		size = subject.size();
		Assert.assertEquals(4, size);
		animal = subject.remove();
		Assert.assertEquals("mice", animal);
		size = subject.size();
		Assert.assertEquals(3, size);
		animal = subject.remove();
		Assert.assertEquals("dogs", animal);
		size = subject.size();
		Assert.assertEquals(2, size);
		animal = subject.remove();
		Assert.assertEquals("cats", animal);
		size = subject.size();
		Assert.assertEquals(1, size);
		animal = subject.remove();
		Assert.assertEquals("birds", animal);
		size = subject.size();
		Assert.assertEquals(0, size);
		animal = subject.remove();
	}

	@Test
	public void testPoll() throws Exception {
		subject.add("cows");
		subject.add("sheep");
		subject.add("ducks");
		subject.add("mice");
		subject.add("dogs");
		subject.add("cats");
		subject.add("birds");
		int size = subject.size();
		Assert.assertEquals(7, size);
		String animal = subject.poll();
		Assert.assertEquals("cows", animal);
		size = subject.size();
		Assert.assertEquals(6, size);
		animal = subject.poll();
		Assert.assertEquals("sheep", animal);
		size = subject.size();
		Assert.assertEquals(5, size);
		animal = subject.poll();
		Assert.assertEquals("ducks", animal);
		size = subject.size();
		Assert.assertEquals(4, size);
		animal = subject.poll();
		Assert.assertEquals("mice", animal);
		size = subject.size();
		Assert.assertEquals(3, size);
		animal = subject.poll();
		Assert.assertEquals("dogs", animal);
		size = subject.size();
		Assert.assertEquals(2, size);
		animal = subject.poll();
		Assert.assertEquals("cats", animal);
		size = subject.size();
		Assert.assertEquals(1, size);
		animal = subject.poll();
		Assert.assertEquals("birds", animal);
		size = subject.size();
		Assert.assertEquals(0, size);
		animal = subject.poll();
		Assert.assertEquals(null, animal);
	}
	@Test
	public void testPeek() throws Exception {
		subject.add("cows");
		subject.add("sheep");
		subject.add("ducks");
		subject.add("mice");
		subject.add("dogs");
		subject.add("cats");
		subject.add("birds");
		int size = subject.size();
		Assert.assertEquals(7, size);
		String animal = subject.peek();
		Assert.assertEquals("cows", animal);
		size = subject.size();
		Assert.assertEquals(7, size);
	}

	@Test
	public void testToArray() throws Exception {
		subject.add("cows");
		subject.add("sheep");
		subject.add("ducks");
		subject.add("mice");
		subject.add("dogs");
		subject.add("cats");
		subject.add("birds");
		int size = subject.size();
		Assert.assertEquals(7, size);
		Object[] animals = subject.toArray();
		Assert.assertEquals(7, animals.length);
	}


}

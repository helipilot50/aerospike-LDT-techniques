package com.aerospike.examples.ldt;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Key;

public class LDTQueueTest {
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
	@Test
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
		
	}


}

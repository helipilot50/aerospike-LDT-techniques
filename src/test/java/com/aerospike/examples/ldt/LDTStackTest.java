package com.aerospike.examples.ldt;

import junit.framework.Assert;

import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Key;

public class LDTStackTest {
	public static final Logger LOG = Logger.getLogger(LDTStackTest.class);
	AerospikeClient client;
	Key key;
	LDTStack<String> subject;

	@Before
	public void setUp() throws Exception {
		client = new AerospikeClient("localhost", 3000);
		key = new Key("test", "demo", "the-stack-001");
		client.delete(null, key);
		subject = new LDTStack<String>(client, key, "the-stack");
	}

	@After
	public void tearDown() throws Exception {
		client.close();
	}

	@Test
	public void testPush() throws Exception {
		subject.push("cows");
		subject.push("sheep");
		subject.push("ducks");
		subject.push("mice");
		subject.push("dogs");
		subject.push("cats");
		subject.push("birds");
		int size = subject.size();
		Assert.assertEquals(7, size);
	}
	
	@Test
	public void testPeek() throws Exception {
		subject.push("cows");
		subject.push("sheep");
		subject.push("ducks");
		String value = subject.peek();
		Assert.assertEquals("ducks", value);
	}

	@Test
	public void testPop() throws Exception {
		subject.push("cows");
		subject.push("sheep");
		subject.push("ducks");
		String value = subject.pop();
		Assert.assertEquals("ducks", value);
	}
	
	@Test
	public void testClear() throws Exception {
		subject.push("cows");
		subject.push("sheep");
		subject.push("ducks");
		subject.push("mice");
		subject.push("dogs");
		subject.push("cats");
		subject.push("birds");
		int size = subject.size();
		Assert.assertEquals(7, size);
		subject.clear();
		size = subject.size();
		Assert.assertEquals(0, size);
		subject.push("birds");
		size = subject.size();
		Assert.assertEquals(1, size);
		
	}

}

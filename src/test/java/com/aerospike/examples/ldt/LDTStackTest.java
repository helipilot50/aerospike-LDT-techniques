package com.aerospike.examples.ldt;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Key;

public class LDTStackTest {
	AerospikeClient client;
	Key key;
	LDTStack<String> subject;

	@Before
	public void setUp() throws Exception {
		client = new AerospikeClient("localhost", 3000);
		key = new Key("test", "demo", "the-map-001");
		subject = new LDTStack<String>(client, key, "the-map");
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
	public void testPop() throws Exception {
		subject.push("cows");
		subject.push("sheep");
		subject.push("ducks");
		String value = subject.pop();
		Assert.assertEquals("ducks", value);
	}

}

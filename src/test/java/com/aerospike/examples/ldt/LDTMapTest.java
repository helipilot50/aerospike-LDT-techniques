package com.aerospike.examples.ldt;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Key;

public class LDTMapTest {

	AerospikeClient client;
	Key key;
	LDTMap<String, Long> subject;

	@Before
	public void setUp() throws Exception {
		client = new AerospikeClient("localhost", 3000);
		key = new Key("test", "demo", "the-map-001");
		client.delete(null, key);
		subject = new LDTMap<String, Long>(client, key, "the-map");
	}

	@After
	public void tearDown() throws Exception {
		client.close();
	}

	@Test
	public void testPut() throws Exception {
		subject.put("cows", 3);
		subject.put("sheep", 18);
		subject.put("ducks", 73);
		subject.put("mice", 36);
		subject.put("dogs", 63);
		subject.put("cats", 43);
		subject.put("birds", 23);
		int size = subject.size();
		Assert.assertEquals(7, size);
	}
	
	@Test
	public void testGet() throws Exception {
		subject.put("cows", 3);
		long result = subject.get("cows");
		Assert.assertEquals(3, result);
	}

}

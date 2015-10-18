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
import java.util.Map.Entry;
import java.util.Set;

import junit.framework.Assert;

import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Log;
import com.aerospike.client.Log.Callback;
import com.aerospike.client.Log.Level;

public class LDTMapTest {
	public static final Logger log = Logger.getLogger(LDTMapTest.class);
	AerospikeClient client;
	Key key;
	LDTMap<String, Long> subject;

	@Before
	public void setUp() throws Exception {
		client = new AerospikeClient("localhost", 3000);
		Log.setCallback(new Callback() {
			
			@Override
			public void log(Level level, String message) {
				switch (level){
				case INFO:
					log.info(message);
					break;
				case WARN:
					log.warn(message);
					break;
				case DEBUG:
					log.debug(message);
					break;
				case ERROR:
					log.error(message);
					break;
				}
				
			}
		});
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

	@Test
	public void testEntrySet() throws Exception {
		subject.put("cows", 3);
		subject.put("sheep", 18);
		subject.put("ducks", 73);
		subject.put("mice", 36);
		subject.put("dogs", 63);
		subject.put("cats", 43);
		subject.put("birds", 23);
		Set<Entry<String, Long>> set = subject.entrySet();
		Assert.assertEquals(7, set.size());
		log.info(set);
	}
	@Test
	public void testValues() throws Exception {
		subject.put("cows", 3);
		subject.put("sheep", 18);
		subject.put("ducks", 73);
		subject.put("mice", 36);
		subject.put("dogs", 63);
		subject.put("cats", 43);
		subject.put("birds", 23);
		Collection<Long> values = subject.values();
		Assert.assertEquals(7, values.size());
		log.info(values);
	}

}

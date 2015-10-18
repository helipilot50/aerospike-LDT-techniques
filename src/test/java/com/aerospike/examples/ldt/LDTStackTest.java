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

public class LDTStackTest {
	public static final Logger log = Logger.getLogger(LDTStackTest.class);
	AerospikeClient client;
	Key key;
	LDTStack<String> subject;

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

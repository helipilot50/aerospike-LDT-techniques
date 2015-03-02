package com.aerospike.examples.ldt;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

@RunWith(Suite.class)
@SuiteClasses({ LDTMapTest.class, LDTQueueTest.class, LDTStackTest.class })
public class AllTests {

}

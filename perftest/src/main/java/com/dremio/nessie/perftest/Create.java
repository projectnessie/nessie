/*
 * Copyright (C) 2017-2019 Dremio Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dremio.nessie.perftest;

import com.dremio.nessie.client.NessieClient;
import com.dremio.nessie.client.NessieClient.AuthType;
import com.dremio.nessie.model.ImmutableBranch;
import org.apache.jmeter.assertions.DurationAssertion;
import org.apache.jmeter.assertions.ResponseAssertion;
import org.apache.jmeter.config.RandomVariableConfig;
import org.apache.jmeter.control.LoopController;
import org.apache.jmeter.engine.StandardJMeterEngine;
import org.apache.jmeter.modifiers.CounterConfig;
import org.apache.jmeter.protocol.java.sampler.JavaSampler;
import org.apache.jmeter.reporters.ResultCollector;
import org.apache.jmeter.testelement.TestPlan;
import org.apache.jmeter.threads.ThreadGroup;
import org.apache.jorphan.collections.HashTree;

public class Create implements Runnable {

  public static RandomVariableConfig randomVariable(String name,
                                                    String variable,
                                                    int top,
                                                    int bottom) {
    RandomVariableConfig randomVariableConfig = new RandomVariableConfig();
    randomVariableConfig.setName(name);
    randomVariableConfig.setProperty("variableName", variable);
    randomVariableConfig.setProperty("maximumValue", Integer.toString(top));
    randomVariableConfig.setProperty("minimumValue", Integer.toString(bottom));
    randomVariableConfig.setProperty("perThread", "true");
    return randomVariableConfig;
  }

  public static CounterConfig counter(String name, String varName, int start) {
    CounterConfig counter = new CounterConfig();
    counter.setProperty("CounterConfig.start", start);
    counter.setProperty("CounterConfig.name", varName);
    counter.setProperty("name", name);
    counter.setProperty("CounterConfig.incr", 1);
    return counter;
  }

  public void run() {

    try {
      new NessieClient(AuthType.BASIC,
                       "http://ec2-35-176-53-79.eu-west-2.compute.amazonaws.com:19131/api/v1",
                       "admin_user",
                       "test123").createBranch(
        ImmutableBranch.builder().name("master").build());
    } catch (Throwable t) {
      //pass
    }
    // Engine
    StandardJMeterEngine jm = new StandardJMeterEngine();
    // jmeter.properties
//    JMeterUtils.loadJMeterProperties("/tmp/jmeter.properties");

    HashTree hashTree = new HashTree();

    CounterConfig counter = counter("counter", "table", 0);
    JavaSampler javaSampler = new JavaSampler();
    javaSampler.setClassname(NessieSampler.class.getCanonicalName());
    javaSampler.setArguments(NessieSampler.getArguments("2",
                                                        "http://ec2-35-176-53-79.eu-west-2.compute.amazonaws.com:19131/api/v1",
                                                        "master",
                                                        "master",
                                                        "table-${table}"));

    // Loop Controller
    LoopController loopController = new LoopController();
    loopController.setLoops(100);
    loopController.addTestElement(javaSampler);
    loopController.setFirst(true);
    loopController.initialize();

    // Thread Group
    ThreadGroup threadGroup = new ThreadGroup();
    threadGroup.setName("test");
    threadGroup.setNumThreads(10);
    threadGroup.setRampUp(1);
    threadGroup.setSamplerController(loopController);
    threadGroup.setEnabled(true);

    // Test plan
    TestPlan testPlan = new TestPlan("MY TEST PLAN");
    testPlan.setEnabled(true);
    hashTree.add(testPlan);
    HashTree threadGroupHashTree = hashTree.add(testPlan, threadGroup);

    //What we test

    DurationAssertion durationAssertion = new DurationAssertion();
    durationAssertion.setName("Duration Assertion 3s per request");
    durationAssertion.setAllowedDuration(3000);

    ResponseAssertion responseAssertion = new ResponseAssertion();
    responseAssertion.setTestFieldResponseCode();
    responseAssertion.setToContainsType();
    responseAssertion.addTestString("200|302");

    threadGroupHashTree.add(javaSampler, durationAssertion);
    threadGroupHashTree.add(javaSampler, responseAssertion);

    ResultCollector jMeterSummarizer = Main.buildJMeterSummarizer("stresstest.csv");

    hashTree.add(testPlan, jMeterSummarizer);
    threadGroupHashTree.add(javaSampler, counter);

    jm.configure(hashTree);

    jm.run();
  }

}

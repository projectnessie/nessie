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

import org.apache.jmeter.assertions.DurationAssertion;
import org.apache.jmeter.assertions.ResponseAssertion;
import org.apache.jmeter.config.RandomVariableConfig;
import org.apache.jmeter.control.LoopController;
import org.apache.jmeter.engine.StandardJMeterEngine;
import org.apache.jmeter.protocol.java.sampler.JavaSampler;
import org.apache.jmeter.reporters.ResultCollector;
import org.apache.jmeter.testelement.TestPlan;
import org.apache.jmeter.threads.ThreadGroup;
import org.apache.jorphan.collections.HashTree;

public class Commit implements Runnable{

  public void run() {
    StandardJMeterEngine jm = new StandardJMeterEngine();

    HashTree hashTree = new HashTree();

    RandomVariableConfig randomVariableConfig = new RandomVariableConfig();
    randomVariableConfig.setName("Operation name");
    randomVariableConfig.setProperty("variableName", "operation");
    randomVariableConfig.setProperty("maximumValue", "10");
    randomVariableConfig.setProperty("minimumValue", "0");
    randomVariableConfig.setProperty("perThread", "true");

    JavaSampler javaSampler = new JavaSampler();
    javaSampler.setClassname(NessieSampler.class.getCanonicalName());
    javaSampler.setArguments(NessieSampler.getArguments("0", "http://localhost:19120/api/v1", "master", null, ""));

    // Loop Controller
    LoopController loopController = new LoopController();
    loopController.setLoops(10);
    loopController.addTestElement(javaSampler);
    loopController.setFirst(true);
    loopController.initialize();

    // Thread Group
    ThreadGroup threadGroup = new ThreadGroup();
    threadGroup.setName("test");
    threadGroup.setNumThreads(1);
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
    threadGroupHashTree.add(javaSampler, randomVariableConfig);

    jm.configure(hashTree);

    jm.run();
  }

}

/*
 * Copyright (C) 2020 Dremio
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dremio.nessie.perftest;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Enumeration;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

import org.apache.jmeter.assertions.DurationAssertion;
import org.apache.jmeter.assertions.ResponseAssertion;
import org.apache.jmeter.config.RandomVariableConfig;
import org.apache.jmeter.control.LoopController;
import org.apache.jmeter.engine.StandardJMeterEngine;
import org.apache.jmeter.modifiers.CounterConfig;
import org.apache.jmeter.protocol.java.sampler.JavaSampler;
import org.apache.jmeter.reporters.ResultCollector;
import org.apache.jmeter.reporters.Summariser;
import org.apache.jmeter.testelement.TestPlan;
import org.apache.jmeter.threads.ThreadGroup;
import org.apache.jmeter.util.JMeterUtils;
import org.apache.jorphan.collections.HashTree;

import com.dremio.nessie.client.NessieClient;
import com.dremio.nessie.client.NessieClient.AuthType;
import com.dremio.nessie.model.ImmutableBranch;

public class Create implements Runnable {

  public static void main(String[] args) throws IOException {
    initialize();
    new Create().run();
  }

  /**
   * build and run JMeter test.
   */
  @Override
  public void run() {

    // Step 1: build nessie client
    try {
      new NessieClient(AuthType.BASIC,
                       "http://localhost:19131/api/v1",
                       "admin_user",
                       "test123").createBranch(
          ImmutableBranch.builder().name("master").build());
    } catch (Throwable t) {
      //pass
    }

    // Engine
    StandardJMeterEngine jm = new StandardJMeterEngine();

    HashTree hashTree = new HashTree();

    // Step 2: create counter to give each table a unique name
    CounterConfig counter = counter("counter", "table", 0);

    // Step 3: set up arguments for each JMH invocation. Counting tables by above counter
    JavaSampler javaSampler = new JavaSampler();
    javaSampler.setClassname(NessieSampler.class.getCanonicalName());
    javaSampler.setArguments(NessieSampler.getArguments("2",
                                                        "http://localhost:19131/api/v1",
                                                        "master",
                                                        "master",
                                                        "table-${table}"));

    // Step 4: Loop Controller - A set of actions a committer will take.
    LoopController loopController = new LoopController();
    loopController.setLoops(100);
    loopController.addTestElement(javaSampler);
    loopController.setFirst(true);
    loopController.initialize();

    // Step 5: Thread Group - The concurrent committers accessing the database
    ThreadGroup threadGroup = new ThreadGroup();
    threadGroup.setName("test");
    threadGroup.setNumThreads(10);
    threadGroup.setRampUp(1);
    threadGroup.setSamplerController(loopController);
    threadGroup.setEnabled(true);

    // Step 6: Test plan -- Build test plan and perform assertions
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

    // Step 7: write results
    ResultCollector summarizer = buildJMeterSummarizer("stresstest.csv");

    hashTree.add(testPlan, summarizer);
    threadGroupHashTree.add(javaSampler, counter);

    // Step 8: Finished configuring, run!
    jm.configure(hashTree);

    jm.run();
  }

  /**
   * Generate random variable config which chooses a random int from top to bottom per thread.
   */
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

  /**
   * generate a monotonicly increasing counter starting at start.
   */
  public static CounterConfig counter(String name, String varName, int start) {
    CounterConfig counter = new CounterConfig();
    counter.setProperty("CounterConfig.start", start);
    counter.setProperty("CounterConfig.name", varName);
    counter.setProperty("name", name);
    counter.setProperty("CounterConfig.incr", 1);
    return counter;
  }

  /**
   * initialize JMeter.
   *
   * <p>
   * First we must find the configs for JMH in its jar then extract the config files to a tmp directory. Once done we point JMH at this
   * config.
   * </p>
   */
  private static void initialize() throws IOException {
    Path workingDirPath = Files.createTempDirectory("demo");
    String s = Create.class.getClassLoader()
                           .getResource("bin/krb5.conf")
                           .getPath()
                           .replace("file:", "")
                           .replace("!/bin/krb5.conf", "");
    try (JarFile jf = new JarFile(s)) {
      Enumeration<JarEntry> entries = jf.entries();
      while (entries.hasMoreElements()) {
        JarEntry je = entries.nextElement();
        String name = je.getName();
        if (je.isDirectory()) {
          // directory found
          Path dir = workingDirPath.resolve(name);
          Files.createDirectory(dir);
        } else {
          Path file = workingDirPath.resolve(name);
          try (InputStream is = jf.getInputStream(je)) {
            Files.copy(is, file, StandardCopyOption.REPLACE_EXISTING);
          }
        }
      }
    }
    String name = "user.properties";
    Path file = workingDirPath.resolve(name);
    try (InputStream is = Create.class.getClassLoader().getResourceAsStream(name)) {
      Files.copy(is, file, StandardCopyOption.REPLACE_EXISTING);
    }
    JMeterUtils.setJMeterHome(workingDirPath.toString());
    JMeterUtils.loadJMeterProperties(file.toString());
    JMeterUtils.initLocale();
  }

  /**
   * build summarizer specific to our usecase. Collects important metrics nad writes to csv.
   */
  public static ResultCollector buildJMeterSummarizer(String logFileName) {
    // add Summarizer output to get progress in stdout:
    Summariser summariser = null;
    String summariserName = JMeterUtils.getPropDefault("summariser.name", "summary");
    if (summariserName.length() > 0) {
      summariser = new Summariser(summariserName);
    }
    summariser.setEnabled(true);
    // Store execution results into a .csv file
    ResultCollector resultCollector = new ResultCollector(summariser);
    resultCollector.setFilename(logFileName);
    resultCollector.setEnabled(true);
    return resultCollector;
  }
}

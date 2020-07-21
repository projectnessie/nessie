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
import org.apache.jmeter.config.Arguments;
import org.apache.jmeter.config.RandomVariableConfig;
import org.apache.jmeter.control.LoopController;
import org.apache.jmeter.control.RandomController;
import org.apache.jmeter.engine.StandardJMeterEngine;
import org.apache.jmeter.protocol.http.sampler.HTTPSamplerProxy;
import org.apache.jmeter.protocol.java.sampler.JavaSampler;
import org.apache.jmeter.report.config.ConfigurationException;
import org.apache.jmeter.report.dashboard.GenerationException;
import org.apache.jmeter.report.dashboard.ReportGenerator;
import org.apache.jmeter.reporters.ResultCollector;
import org.apache.jmeter.reporters.Summariser;
import org.apache.jmeter.testelement.TestPlan;
import org.apache.jmeter.threads.ThreadGroup;
import org.apache.jmeter.util.JMeterUtils;
import org.apache.jorphan.collections.HashTree;

public class Main {

  private static void initialize() throws IOException {
    Path workingDirPath = Files.createTempDirectory("demo");
    String s = Main.class.getClassLoader()
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
    try (InputStream is = Main.class.getClassLoader().getResourceAsStream(name)) {
      Files.copy(is, file, StandardCopyOption.REPLACE_EXISTING);
    }
    JMeterUtils.setJMeterHome(workingDirPath.toString());
    JMeterUtils.loadJMeterProperties(file.toString());
    JMeterUtils.initLocale();
  }

  public static void main(String[] args) throws IOException {
    initialize();
    new Create().run();
  }

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

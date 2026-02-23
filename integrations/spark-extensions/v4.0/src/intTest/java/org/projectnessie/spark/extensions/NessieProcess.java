/*
 * Copyright (C) 2024 Dremio
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
package org.projectnessie.spark.extensions;

import static java.lang.String.format;
import static java.lang.System.getProperty;
import static java.util.Arrays.asList;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import org.projectnessie.nessierunner.common.ProcessHandler;

/**
 * Manages the Nessie process.
 *
 * <p>Since Nessie Catalog (Iceberg REST) does not allow the usage of {@code file:} based
 * warehouses, we can no longer use the nessie-quarkus-runner Gradle plugin, because we now also
 * need an object store - and the endpoint of the object store needs to be passed to Nessie Catalog,
 * which is rather not doable in a build script.
 *
 * <p>We leverage the code to manage a process from nessie-quarkus-runner but start the process from
 * the JVM running the test, while having a best-effort implementation to make sure that the process
 * eventually terminates: via JUnit's {@code @AfterAll}, via a shutdown hook and the JVM
 * self-destruct option {@code -XX:SelfDestructTimer=30}.
 */
final class NessieProcess {
  private NessieProcess() {}

  static ProcessHandler processHandler;
  static String baseUri;

  static void start(String... args) throws Exception {
    if (processHandler != null) {
      throw new IllegalStateException("Already started");
    }

    String execJar = getProperty("nessie.exec-jar");
    String javaExec = getProperty("java-exec");
    if (javaExec == null) {
      javaExec = getProperty("java.home") + "/bin/java";
    }

    List<String> command = new ArrayList<>();
    command.add(javaExec);
    command.add("-XX:SelfDestructTimer=30");
    command.add("-Dquarkus.http.port=0");
    command.add("-Dquarkus.management.port=0");
    command.add("-Dquarkus.http.port=0");
    command.add("-Dquarkus.management.port=0");
    command.add("-Dnessie.server.send-stacktrace-to-client=true");
    command.addAll(asList(args));
    command.add("-jar");
    command.add(execJar);

    processHandler = new ProcessHandler();
    processHandler.start(new ProcessBuilder().command(command));

    Runtime.getRuntime().addShutdownHook(new Thread(NessieProcess::stop));

    List<String> listenUrls = processHandler.getListenUrls();
    String httpListenUrl = listenUrls.get(0);
    int httpListenPort = URI.create(httpListenUrl).getPort();
    baseUri = format("http://127.0.0.1:%s/", httpListenPort);
  }

  public static void stop() {
    if (processHandler != null) {
      try {
        processHandler.stop();
      } finally {
        processHandler = null;
      }
    }
  }
}

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
package org.projectnessie.quarkus.gradle;

import java.net.URL;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import org.gradle.api.DefaultTask;
import org.gradle.api.tasks.TaskAction;
import org.gradle.api.tasks.testing.Test;

public class StartTask extends DefaultTask {

  public StartTask() {
    // intentionally empty
  }

  @TaskAction
  public void noop() {
  }

  @SuppressWarnings("UnstableApiUsage") // omit warning about `Property`+`MapProperty`
  void quarkusStart(Test testTask) {
    getLogger().info("Starting Quarkus application.");

    QuarkusAppExtension extension = getProject().getExtensions().getByType(QuarkusAppExtension.class);

    // This is not doing anything with Docker or building a native image, just a quirk of Quarkus since 1.10.
    System.setProperty("quarkus.native.builder-image", extension.getNativeBuilderImageProperty().get());

    Map<String, Object> props = extension.getPropsProperty().get();

    Properties properties = new Properties();
    properties.putAll(props);

    // Prepare/configure logging (log level defaults to "info", can be overridden via the
    // environment variable NESSIE_QUARKUS_LOG_LEVEL
    if (!System.getProperties().containsKey("java.util.logging.manager")) {
      System.setProperty("java.util.logging.manager", "org.jboss.logmanager.LogManager");
    }
    if (!System.getProperties().containsKey("log4j2.configurationFile")) {
      URL log4j2config = StartTask.class.getResource("/org/projectnessie/quarkus/gradle/log4j2-quarkus.xml");

      System.setProperty("log4j2.configurationFile", log4j2config.toString());
    }

    AutoCloseable quarkusApp = QuarkusApp.newApplication(getProject(), properties);

    // Do not put the "dynamic" properties (quarkus.http.test-port) to the `Test` task's
    // system-properties, because those are subject to the test-task's inputs, which is used
    // as the build-cache key. Instead, pass the dynamic properties via a CommandLineArgumentProvider.
    // In other words: ensure that the `Test` tasks is cacheable.
    testTask.getJvmArgumentProviders().add(() -> props.keySet().stream()
        .filter(k -> System.getProperty(k) != null)
        .map(k -> String.format("-D%s=%s", k, System.getProperty(k)))
        .collect(Collectors.toList()));

    getLogger().info("Quarkus application started.");
    setApplicationHandle(quarkusApp);
  }

  private void setApplicationHandle(AutoCloseable application) {
    // update stop task with this task's closeable

    StopTask task = (StopTask) getProject().getTasks().getByName(QuarkusAppPlugin.STOP_TASK_NAME);
    if (task.getApplication() != null) {
      getLogger().warn("StopTask application is not empty!");
    }
    task.setQuarkusApplication(application);
  }
}

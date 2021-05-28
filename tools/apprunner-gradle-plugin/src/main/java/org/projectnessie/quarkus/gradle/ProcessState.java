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

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import java.util.Set;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import org.gradle.api.DefaultTask;
import org.gradle.api.GradleException;
import org.gradle.api.artifacts.Configuration;
import org.gradle.api.artifacts.Dependency;
import org.gradle.api.artifacts.DependencySet;
import org.gradle.api.artifacts.ModuleDependency;
import org.gradle.api.file.RegularFile;
import org.gradle.api.tasks.TaskAction;
import org.gradle.api.tasks.testing.Test;
import org.projectnessie.quarkus.runner.JavaVM;
import org.projectnessie.quarkus.runner.ProcessHandler;

public class ProcessState {

  private ProcessHandler processHandler;

  public ProcessState() {
    // intentionally empty
  }

  @TaskAction
  public void noop() {
  }

  @SuppressWarnings("UnstableApiUsage") // omit warning about `Property`+`MapProperty`
  void quarkusStart(Test testTask) {
    QuarkusAppExtension extension = testTask.getProject().getExtensions().getByType(QuarkusAppExtension.class);

    Configuration appConfig = testTask.getProject().getConfigurations().getByName(QuarkusAppPlugin.APP_CONFIG_NAME);

    RegularFile configuredJar = extension.getExecutableJar().getOrNull();

    DependencySet dependencies = appConfig.getDependencies();
    File execJar;

    if (configuredJar == null) {
      if (dependencies.isEmpty()) {
        throw new GradleException(String
            .format("Dependency org.projectnessie:nessie-quarkus:runner missing in configuration %s", QuarkusAppPlugin.APP_CONFIG_NAME));
      }
      if (dependencies.size() != 1) {
        throw new GradleException(String.format(
            "Configuration %s must only contain the org.projectnessie:nessie-quarkus:runner dependency, but resolves to these artifacts: %s",
            QuarkusAppPlugin.APP_CONFIG_NAME,
            dependencies.stream().map(d -> String.format("%s:%s:%s", d.getGroup(), d.getName(), d.getVersion()))
                .collect(Collectors.joining(", "))));
      }
      Dependency dep = dependencies.iterator().next();
      if (!(dep instanceof ModuleDependency)) {
        throw new GradleException(
            String.format("Expected dependency %s to be of type ModuleDependency, but is %s", dep, dep.getClass().getSimpleName()));
      }
      Set<File> files = appConfig.resolve();
      if (files.size() != 1) {
        throw new GradleException(String.format(
            "Expected configuration %s with dependency %s to resolve to exactly one artifact, but resolves to %s (hint: do not enable transitive on the dependency)",
            QuarkusAppPlugin.APP_CONFIG_NAME, dep, files));
      }
      execJar = files.iterator().next();
    } else {
      if (!dependencies.isEmpty()) {
        throw new GradleException(String
            .format("Configuration %s contains a dependency and option 'executableJar' are mutually exclusive",
                QuarkusAppPlugin.APP_CONFIG_NAME));
      }
      execJar = configuredJar.getAsFile();
    }

    JavaVM javaVM = JavaVM.findJavaVM(extension.getJavaVersion().get());
    if (javaVM == null) {
      throw new GradleException(noJavaMessage(extension.getJavaVersion().get()));
    }

    Path workDir = extension.getWorkingDirectory().getAsFile().get().toPath();
    if (!Files.isDirectory(workDir)) {
      try {
        Files.createDirectories(workDir);
      } catch (IOException e) {
        throw new GradleException(String.format("Failed to create working directory %s", workDir), e);
      }
    }

    List<String> command = new ArrayList<>();
    command.add(javaVM.getJavaExecutable().toString());
    command.addAll(extension.getJvmArguments().get());
    extension.getSystemProperties().get().forEach((k, v) -> command.add(String.format("-D%s=%s", k, v)));
    command.add("-Dquarkus.http.port=0");
    command.add("-jar");
    command.add(execJar.getAbsolutePath());
    command.addAll(extension.getArguments().get());

    if (testTask.getLogger().isDebugEnabled()) {
      testTask.getLogger().debug("Starting process: {}", command);
    } else {
      testTask.getLogger().info("Running jar {} with {}", execJar, javaVM.getJavaExecutable());
    }

    ProcessBuilder processBuilder = new ProcessBuilder()
        .command(command);
    extension.getEnvironment().get().forEach((k, v) -> processBuilder.environment().put(k, v));
    processBuilder.directory(workDir.toFile());

    try {
      processHandler = new ProcessHandler();
      processHandler.start(processBuilder);
      if (extension.getTimeToListenUrlMillis().get() > 0L) {
        processHandler.setTimeToListenUrlMillis(extension.getTimeToListenUrlMillis().get());
      }
      if (extension.getTimeToStopMillis().get() > 0L) {
        processHandler.setTimeStopMillis(extension.getTimeToStopMillis().get());
      }
      processHandler.getListenUrl();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new GradleException(String.format("Process-start interrupted: %s", command), e);
    } catch (TimeoutException e) {
      throw new GradleException(String.format("Nessie-Server/Quarkus did not emit listen URL. Process: %s", command), e);
    } catch (IOException e) {
      throw new GradleException(String.format("Failed to start the process %s", command), e);
    }

    String listenUrl;
    try {
      listenUrl = processHandler.getListenUrl();
    } catch (Exception e) {
      // Can safely ignore it (it invocation does not block and therefore not throw an exception).
      // But make the IDE happy with this throw.
      throw new RuntimeException(e);
    }
    String listenPort = Integer.toString(URI.create(listenUrl).getPort());

    // Do not put the "dynamic" properties (quarkus.http.test-port) to the `Test` task's
    // system-properties, because those are subject to the test-task's inputs, which is used
    // as the build-cache key. Instead, pass the dynamic properties via a CommandLineArgumentProvider.
    // In other words: ensure that the `Test` tasks is cacheable.
    testTask.getJvmArgumentProviders().add(() -> Arrays.asList(
      String.format("-D%s=%s", extension.getHttpListenUrlProperty().get(), listenUrl),
      String.format("-D%s=%s", extension.getHttpListenPortProperty().get(), listenPort))
    );
  }

  void quarkusStop(Test testTask) {
    if (processHandler == null) {
      testTask.getLogger().debug("No application found.");
      return;
    }

    try {
      processHandler.stop();
      testTask.getLogger().info("Quarkus application stopped.");
    } catch (Exception e) {
      throw new RuntimeException(e);
    } finally {
      processHandler = null;
    }
  }

  //@VisibleForTesting
  static String noJavaMessage(int version) {
    return String.format("Could not find a Java-VM for Java version %d. "
            + "Set the Java-Home for a compatible JVM using the environment variable JDK%d_HOME or "
            + "JAVA%d_HOME.",
        version, version, version);
  }
}

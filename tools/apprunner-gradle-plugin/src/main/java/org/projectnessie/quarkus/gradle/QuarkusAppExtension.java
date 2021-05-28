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
import java.util.Collections;
import org.gradle.api.Project;
import org.gradle.api.file.RegularFileProperty;
import org.gradle.api.provider.ListProperty;
import org.gradle.api.provider.MapProperty;
import org.gradle.api.provider.Property;

@SuppressWarnings("UnstableApiUsage") // omit warning about `Property`+`MapProperty`
public class QuarkusAppExtension {
  private final MapProperty<String, String> environment;
  private final MapProperty<String, String> systemProperties;
  private final ListProperty<String> arguments;
  private final ListProperty<String> jvmArguments;
  private final Property<Integer> javaVersion;
  private final Property<String> httpListenPortProperty;
  private final Property<String> httpListenUrlProperty;
  private final RegularFileProperty executableJar;
  private final RegularFileProperty workingDirectory;
  private final Property<Long> timeToListenUrlMillis;
  private final Property<Long> timeToStopMillis;

  public QuarkusAppExtension(Project project) {
    environment = project.getObjects().mapProperty(String.class, String.class);
    systemProperties = project.getObjects().mapProperty(String.class, String.class).convention(
      Collections.singletonMap("quarkus.http.port", "0"));
    arguments = project.getObjects().listProperty(String.class);
    jvmArguments = project.getObjects().listProperty(String.class);
    javaVersion = project.getObjects().property(Integer.class).convention(11);
    httpListenUrlProperty = project.getObjects().property(String.class).convention("quarkus.http.test-url");
    httpListenPortProperty = project.getObjects().property(String.class).convention("quarkus.http.test-port");
    workingDirectory = project.getObjects().fileProperty().convention(() -> new File(project.getBuildDir(), "nessie-quarkus"));
    executableJar = project.getObjects().fileProperty();
    timeToListenUrlMillis = project.getObjects().property(Long.class).convention(0L);
    timeToStopMillis = project.getObjects().property(Long.class).convention(0L);
  }

  public MapProperty<String, String> getSystemProperties() {
    return systemProperties;
  }

  public MapProperty<String, String> getEnvironment() {
    return environment;
  }

  public ListProperty<String> getArguments() {
    return arguments;
  }

  public ListProperty<String> getJvmArguments() {
    return jvmArguments;
  }

  public Property<Integer> getJavaVersion() {
    return javaVersion;
  }

  public Property<String> getHttpListenPortProperty() {
    return httpListenPortProperty;
  }

  public Property<String> getHttpListenUrlProperty() {
    return httpListenUrlProperty;
  }

  public RegularFileProperty getExecutableJar() {
    return executableJar;
  }

  public RegularFileProperty getWorkingDirectory() {
    return workingDirectory;
  }

  public Property<Long> getTimeToListenUrlMillis() {
    return timeToListenUrlMillis;
  }

  public Property<Long> getTimeToStopMillis() {
    return timeToStopMillis;
  }
}

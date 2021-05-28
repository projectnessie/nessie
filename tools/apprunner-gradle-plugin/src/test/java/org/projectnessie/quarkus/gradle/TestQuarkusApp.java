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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.Streams;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.gradle.testkit.runner.BuildResult;
import org.gradle.testkit.runner.GradleRunner;
import org.gradle.testkit.runner.TaskOutcome;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Test for the {@link QuarkusAppPlugin}, which basically simulates what the {@code build.gradle} in
 * Apache Iceberg does.
 */
class TestQuarkusApp {
  @TempDir Path testProjectDir;

  Path buildFile;

  String nessieVersion;
  String quarkusVersion;

  List<String> prefix;

  @BeforeEach
  void setup() throws Exception {
    buildFile = testProjectDir.resolve("build.gradle");
    Path localBuildCacheDirectory = testProjectDir.resolve(".local-cache");

    // Copy our test class in the test's project test-source folder
    Path testTargetDir = testProjectDir.resolve("src/test/java/org/projectnessie/quarkus/gradle");
    Files.createDirectories(testTargetDir);
    Files.copy(
        Paths.get("src/test/resources/org/projectnessie/quarkus/gradle/TestTest.java"),
        testTargetDir.resolve("TestTest.java"));

    Files.write(
        testProjectDir.resolve("settings.gradle"),
        Arrays.asList(
            "buildCache {",
            "    local {",
            "        directory '" + localBuildCacheDirectory.toUri() + "'",
            "    }",
            "}",
            "",
            "include 'sub'"));

    // Versions injected from build.gradle
    nessieVersion = System.getProperty("nessie-version");
    quarkusVersion = System.getProperty("quarkus-version");
    String junitVersion = System.getProperty("junit-version");
    String jacksonVersion = System.getProperty("jackson-version");

    assertTrue(
        nessieVersion != null
            && quarkusVersion != null
            && junitVersion != null
            && jacksonVersion != null,
        "System property required for this test is missing, run this test via Gradle or set the system properties manually");

    prefix =
        Arrays.asList(
            "plugins {",
            "    id 'java'",
            "    id 'org.projectnessie'",
            "}",
            "",
            "repositories {",
            "    mavenLocal()",
            "    mavenCentral()",
            "}",
            "",
            "test {",
            "    useJUnitPlatform()",
            "}",
            "",
            "dependencies {",
            "    testCompile 'org.junit.jupiter:junit-jupiter-api:" + junitVersion + "'",
            "    testCompile 'org.junit.jupiter:junit-jupiter-engine:" + junitVersion + "'",
            "    testCompile 'com.fasterxml.jackson.core:jackson-databind:" + jacksonVersion + "'",
            "    testCompile 'org.projectnessie:nessie-client:" + nessieVersion + "'");
  }

  /**
   * Ensure that the plugin fails when there is no dependency specified for the {@code
   * nessieQuarkusServer} configuration.
   */
  @Test
  void noAppConfigDeps() throws Exception {
    Files.write(
        buildFile, Streams.concat(prefix.stream(), Stream.of("}")).collect(Collectors.toList()));

    failingBuild();
  }

  /**
   * Ensure that the plugin fails when there is more than one dependency specified for the {@code
   * nessieQuarkusServer} configuration.
   */
  @Test
  void tooManyAppConfigDeps() throws Exception {
    Files.write(
        buildFile,
        Streams.concat(
                prefix.stream(),
                Stream.of(
                    "    nessieQuarkusServer 'org.projectnessie:nessie-quarkus:"
                        + nessieVersion
                        + "'",
                    "    nessieQuarkusServer(enforcedPlatform('io.quarkus:quarkus-bom:"
                        + quarkusVersion
                        + "'))",
                    "}"))
            .collect(Collectors.toList()));

    failingBuild();
  }

  /**
   * Starting the Nessie-Server via the Nessie-Quarkus-Gradle-Plugin must work fine, if the
   * quarkus-bom dependency is explicitly specified, although there are conflicting dependencies
   * declared.
   */
  @Test
  void conflictingDependenciesQuarkus() throws Exception {
    Files.write(
        buildFile,
        Streams.concat(
                prefix.stream(),
                Stream.of(
                    "    compile 'io.quarkus:quarkus-bom:1.11.0.Final'",
                    "    nessieQuarkusServer 'org.projectnessie:nessie-quarkus:"
                        + nessieVersion
                        + "'",
                    "    nessieQuarkusRuntime(enforcedPlatform('io.quarkus:quarkus-bom:"
                        + quarkusVersion
                        + "'))",
                    "}"))
            .collect(Collectors.toList()));

    workingBuild();
  }

  /**
   * Starting the Nessie-Server via the Nessie-Quarkus-Gradle-Plugin must work fine, if the
   * quarkus-bom dependency is explicitly specified, although there are conflicting dependencies
   * declared.
   */
  @Test
  void conflictingDependenciesQuarkusEnforced() throws Exception {
    Files.write(
        buildFile,
        Streams.concat(
                prefix.stream(),
                Stream.of(
                    "    compile(enforcedPlatform('io.quarkus:quarkus-bom:1.11.0.Final'))",
                    "    nessieQuarkusServer 'org.projectnessie:nessie-quarkus:"
                        + nessieVersion
                        + "'",
                    "    nessieQuarkusRuntime(enforcedPlatform('io.quarkus:quarkus-bom:"
                        + quarkusVersion
                        + "'))",
                    "}"))
            .collect(Collectors.toList()));

    workingBuild();
  }

  /**
   * Starting the Nessie-Server via the Nessie-Quarkus-Gradle-Plugin must work fine, if the
   * quarkus-bom dependency is explicitly specified, although there are conflicting dependencies
   * declared.
   */
  @Test
  void conflictingDependenciesNessie() throws Exception {
    Files.write(
        buildFile,
        Streams.concat(
                prefix.stream(),
                Stream.of(
                    "    compile 'org.projectnessie:nessie-client:0.4.0'",
                    "    nessieQuarkusServer 'org.projectnessie:nessie-quarkus:"
                        + nessieVersion
                        + "'",
                    "    nessieQuarkusRuntime(enforcedPlatform('io.quarkus:quarkus-bom:"
                        + quarkusVersion
                        + "'))",
                    "}"))
            .collect(Collectors.toList()));

    workingBuild();
  }

  /**
   * Starting the Nessie-Server via the Nessie-Quarkus-Gradle-Plugin does NOT WORK, if there is no
   * enforced-platform with the quarkus-bom dependency.
   */
  @Test
  void noConflictingDependencies() throws Exception {
    Files.write(
        buildFile,
        Streams.concat(
                prefix.stream(),
                Stream.of(
                    "    nessieQuarkusServer 'org.projectnessie:nessie-quarkus:"
                        + nessieVersion
                        + "'",
                    "}"))
            .collect(Collectors.toList()));

    failingBuild();
  }

  @SuppressWarnings("ConstantConditions") // prevent IntelliJ NPE warning
  private void workingBuild() {
    BuildResult result =
        GradleRunner.create()
            .withPluginClasspath()
            .withProjectDir(testProjectDir.toFile())
            .withArguments("--build-cache", "--info", "test")
            .forwardOutput()
            .build();

    assertEquals(TaskOutcome.SUCCESS, result.task(":test").getOutcome());
  }

  @SuppressWarnings("ConstantConditions") // prevent IntelliJ NPE warning
  private void failingBuild() {
    BuildResult result =
        GradleRunner.create()
            .withPluginClasspath()
            .withProjectDir(testProjectDir.toFile())
            .withArguments("--build-cache", "--info", "test")
            .forwardOutput()
            .buildAndFail();

    assertEquals(TaskOutcome.FAILED, result.task(":test").getOutcome());
  }
}

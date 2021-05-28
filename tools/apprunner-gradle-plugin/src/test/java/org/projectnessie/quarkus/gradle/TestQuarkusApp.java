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

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.gradle.testkit.runner.BuildResult;
import org.gradle.testkit.runner.BuildTask;
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

  String nessieVersionForTest;

  List<String> prefix;

  @BeforeEach
  void setup() throws Exception {
    buildFile = testProjectDir.resolve("build.gradle");
    Path localBuildCacheDirectory = testProjectDir.resolve(".local-cache");

    // Copy our test class in the test's project test-source folder
    Path testTargetDir = testProjectDir.resolve("src/test/java/org/projectnessie/quarkus/gradle");
    Files.createDirectories(testTargetDir);
    Files.copy(
        Paths.get(
            "src/test/resources/org/projectnessie/quarkus/gradle/TestSimulatingTestUsingThePlugin.java"),
        testTargetDir.resolve("TestSimulatingTestUsingThePlugin.java"));

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
    nessieVersionForTest = System.getProperty("nessie-version-for-test", "0.6.1");
    String junitVersion = System.getProperty("junit-version");
    String jacksonVersion = System.getProperty("jackson-version");

    assertThat(junitVersion != null && jacksonVersion != null)
        .withFailMessage(
            "System property required for this test is missing, run this test via Gradle or set the system properties manually")
        .isTrue();

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
            "    testCompile 'org.projectnessie:nessie-client:" + nessieVersionForTest + "'");
  }

  /**
   * Ensure that the plugin fails when there is no dependency specified for the {@code
   * nessieQuarkusServer} configuration.
   */
  @Test
  void noAppConfigDeps() throws Exception {
    Files.write(
        buildFile, Streams.concat(prefix.stream(), Stream.of("}")).collect(Collectors.toList()));

    BuildResult result = createGradleRunner("test").buildAndFail();
    assertThat(result.task(":test"))
        .isNotNull()
        .extracting(BuildTask::getOutcome)
        .isEqualTo(TaskOutcome.FAILED);
    assertThat(Arrays.asList(result.getOutput().split("\n")))
        .contains(
            "> Dependency org.projectnessie:nessie-quarkus:runner missing in configuration nessieQuarkusServer");
  }

  /**
   * Ensure that the plugin fails when there is more than one dependency specified for the {@code
   * nessieQuarkusServer} configuration.
   */
  @Test
  void tooManyAppConfigDeps() throws Exception {
    Files.write(
        buildFile,
        Stream.concat(
                prefix.stream(),
                Stream.of(
                    "    nessieQuarkusServer 'org.projectnessie:nessie-quarkus:"
                        + nessieVersionForTest
                        + ":runner'",
                    "    nessieQuarkusServer 'org.projectnessie:nessie-model:"
                        + nessieVersionForTest
                        + "'",
                    "}"))
            .collect(Collectors.toList()));

    BuildResult result = createGradleRunner("test").buildAndFail();
    assertThat(result.task(":test"))
        .isNotNull()
        .extracting(BuildTask::getOutcome)
        .isEqualTo(TaskOutcome.FAILED);
    assertThat(Arrays.asList(result.getOutput().split("\n")))
        .contains(
            "> Configuration nessieQuarkusServer must only contain the org.projectnessie:nessie-quarkus:runner dependency, "
                + "but resolves to these artifacts: "
                + "org.projectnessie:nessie-quarkus:"
                + nessieVersionForTest
                + ", "
                + "org.projectnessie:nessie-model:"
                + nessieVersionForTest);
  }

  /**
   * Ensure that the plugin fails when both the config-dependency and the exec-jar are specified.
   */
  @Test
  void configAndExecJar() throws Exception {
    Files.write(
        buildFile,
        Stream.concat(
                prefix.stream(),
                Stream.of(
                    "    nessieQuarkusServer 'org.projectnessie:nessie-quarkus:"
                        + nessieVersionForTest
                        + ":runner'",
                    "}",
                    "nessieQuarkusApp {",
                    "    executableJar.set(file('/foo/bar/jar'))",
                    "}"))
            .collect(Collectors.toList()));

    BuildResult result = createGradleRunner("test").buildAndFail();
    assertThat(result.task(":test"))
        .isNotNull()
        .extracting(BuildTask::getOutcome)
        .isEqualTo(TaskOutcome.FAILED);
    assertThat(Arrays.asList(result.getOutput().split("\n")))
        .contains(
            "> Configuration nessieQuarkusServer contains a dependency and option 'executableJar' are mutually exclusive");
  }

  /** Ensure that the plugin fails when it doesn't find a matching Java. */
  @Test
  void unknownJdk() throws Exception {
    Files.write(
        buildFile,
        Stream.concat(
                prefix.stream(),
                Stream.of(
                    "    nessieQuarkusServer 'org.projectnessie:nessie-quarkus:"
                        + nessieVersionForTest
                        + ":runner'",
                    "}",
                    "nessieQuarkusApp {",
                    "    javaVersion.set(42)",
                    "}"))
            .collect(Collectors.toList()));

    BuildResult result = createGradleRunner("test").buildAndFail();
    assertThat(result.task(":test"))
        .isNotNull()
        .extracting(BuildTask::getOutcome)
        .isEqualTo(TaskOutcome.FAILED);
    assertThat(Arrays.asList(result.getOutput().split("\n")))
        .contains("> " + ProcessState.noJavaMessage(42));
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

    BuildResult result = createGradleRunner("test").build();
    assertThat(result.task(":test"))
        .isNotNull()
        .extracting(BuildTask::getOutcome)
        .isEqualTo(TaskOutcome.SUCCESS);

    assertThat(Arrays.asList(result.getOutput().split("\n")))
        .anyMatch(l -> l.contains("Listening on: http://0.0.0.0:"))
        .contains("Quarkus application stopped.");

    // 2nd run must be up-to-date

    result = createGradleRunner("test").build();
    assertThat(result.task(":test"))
        .isNotNull()
        .extracting(BuildTask::getOutcome)
        .isEqualTo(TaskOutcome.UP_TO_DATE);

    // 3rd run after a 'clean' must use the cached result

    result = createGradleRunner("clean").build();
    assertThat(result.task(":clean"))
        .isNotNull()
        .extracting(BuildTask::getOutcome)
        .isEqualTo(TaskOutcome.SUCCESS);

    result = createGradleRunner("test").build();
    assertThat(result.task(":test"))
        .isNotNull()
        .extracting(BuildTask::getOutcome)
        .isEqualTo(TaskOutcome.FROM_CACHE);
  }

  private GradleRunner createGradleRunner(String task) {
    return GradleRunner.create()
        .withPluginClasspath()
        .withProjectDir(testProjectDir.toFile())
        .withArguments("--build-cache", "--info", "--stacktrace", task)
        .forwardOutput();
  }
}

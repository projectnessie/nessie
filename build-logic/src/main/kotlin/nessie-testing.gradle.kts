/*
 * Copyright (C) 2023 Dremio
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

import org.gradle.api.attributes.TestSuiteType
import org.gradle.api.component.AdhocComponentWithVariants
import org.gradle.api.plugins.jvm.JvmTestSuite
import org.gradle.api.services.BuildService
import org.gradle.api.services.BuildServiceParameters
import org.gradle.api.tasks.testing.Test
import org.gradle.process.CommandLineArgumentProvider

plugins {
  `java-test-fixtures`
  `jvm-test-suite`
}

gradle.sharedServices.registerIfAbsent(
  "intTestParallelismConstraint",
  TestingParallelismHelper::class.java
) {
  val intTestParallelism =
    Integer.getInteger(
      "nessie.intTestParallelism",
      (Runtime.getRuntime().availableProcessors() / 4).coerceAtLeast(1)
    )
  maxParallelUsages = intTestParallelism
}

gradle.sharedServices.registerIfAbsent(
  "testParallelismConstraint",
  TestingParallelismHelper::class.java
) {
  val intTestParallelism =
    Integer.getInteger(
      "nessie.testParallelism",
      (Runtime.getRuntime().availableProcessors() / 2).coerceAtLeast(1)
    )
  maxParallelUsages = intTestParallelism
}

// Do not publish test fixtures via Maven. Shared, reusable test code should be published as
// a separate project to retain dependency information.
components {
  named("java", AdhocComponentWithVariants::class) {
    withVariantsFromConfiguration(configurations["testFixturesApiElements"]) { skip() }
    withVariantsFromConfiguration(configurations["testFixturesRuntimeElements"]) { skip() }
  }
}

if (plugins.hasPlugin("io.quarkus")) {
  // This directory somehow disappears... Maybe some weird Quarkus code.
  val testFixturesDir = layout.buildDirectory.dir("resources/testFixtures")
  tasks.named("quarkusGenerateCodeTests").configure { doFirst { mkdir(testFixturesDir) } }
  tasks.withType<Test>().configureEach { doFirst { mkdir(testFixturesDir) } }
}

tasks.withType<Test>().configureEach {
  val testJvmArgs: String? by project
  val testHeapSize: String? by project
  jvmArgs("-XX:+HeapDumpOnOutOfMemoryError")
  if (testJvmArgs != null) {
    jvmArgs((testJvmArgs as String).split(" "))
  }

  systemProperty("file.encoding", "UTF-8")
  systemProperty("user.language", "en")
  systemProperty("user.country", "US")
  systemProperty("user.variant", "")

  jvmArgumentProviders.add(
    CommandLineArgumentProvider {
      listOf(
        "-Dtest.log.level=${testLogLevel()}",
        "-Djunit.platform.reporting.open.xml.enabled=true",
        "-Djunit.platform.reporting.output.dir=${reports.junitXml.outputLocation.get().asFile.absolutePath}",
        "-Djunit.jupiter.execution.timeout.default=5m"
      )
    }
  )
  environment("TESTCONTAINERS_REUSE_ENABLE", "true")

  if (plugins.hasPlugin("io.quarkus")) {
    systemProperty("java.util.logging.manager", "org.jboss.logmanager.LogManager")

    jvmArgs("--add-opens=java.base/java.util=ALL-UNNAMED")
    // Log-levels are required to be able to parse the HTTP listen URL
    jvmArgumentProviders.add(
      CommandLineArgumentProvider {
        listOf(
          "-Dquarkus.log.level=${testLogLevel("INFO")}",
          "-Dquarkus.log.console.level=${testLogLevel("INFO")}",
          "-Dhttp.access.log.level=${testLogLevel()}"
        )
      }
    )

    minHeapSize = if (testHeapSize != null) testHeapSize as String else "768m"
    maxHeapSize = if (testHeapSize != null) testHeapSize as String else "2048m"
  } else if (testHeapSize != null) {
    minHeapSize = testHeapSize!!
    maxHeapSize = testHeapSize!!
  }

  filter { isFailOnNoMatchingTests = false }
}

testing {
  suites {
    val test =
      named<JvmTestSuite>("test") {
        useJUnitJupiter(libsRequiredVersion("junit"))

        targets.all {
          testTask.configure {
            usesService(
              gradle.sharedServices.registrations.named("testParallelismConstraint").get().service
            )
          }
        }
      }

    register<JvmTestSuite>("intTest") {
      useJUnitJupiter(libsRequiredVersion("junit"))

      testType = TestSuiteType.INTEGRATION_TEST

      dependencies { implementation.add(project()) }

      val hasQuarkus = plugins.hasPlugin("io.quarkus")

      targets.all {
        testTask.configure {
          usesService(
            gradle.sharedServices.registrations.named("intTestParallelismConstraint").get().service
          )

          shouldRunAfter(test)

          systemProperty("nessie.integrationTest", "true")

          // For Quarkus...
          //
          // io.quarkus.test.junit.IntegrationTestUtil.determineBuildOutputDirectory(java.net.URL)
          // is not smart enough :(
          if (hasQuarkus) {
            systemProperty("build.output.directory", layout.buildDirectory.asFile.get())
            dependsOn(tasks.named("quarkusBuild"))
          }
        }

        if (hasQuarkus) {
          tasks.named("compileIntTestJava").configure {
            dependsOn(tasks.named("compileQuarkusTestGeneratedSourcesJava"))
          }
        }

        tasks.named("check").configure { dependsOn(testTask) }
      }

      if (hasQuarkus) {
        sources { java.srcDirs(tasks.named("quarkusGenerateCodeTests")) }
      }
    }
  }
}

// Let the test's implementation config extend testImplementation, so it also inherits the
// project's "main" implementation dependencies (not just the "api" configuration)
configurations.named("intTestImplementation").configure {
  extendsFrom(configurations.getByName("testImplementation"))
}

dependencies { add("intTestImplementation", java.sourceSets.getByName("test").output.dirs) }

configurations.named("intTestRuntimeOnly").configure {
  extendsFrom(configurations.getByName("testRuntimeOnly"))
}

abstract class TestingParallelismHelper : BuildService<BuildServiceParameters.None>

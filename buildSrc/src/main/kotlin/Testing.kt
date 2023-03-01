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

import org.gradle.api.Project
import org.gradle.api.services.BuildService
import org.gradle.api.services.BuildServiceParameters
import org.gradle.api.tasks.testing.Test
import org.gradle.kotlin.dsl.named
import org.gradle.kotlin.dsl.provideDelegate
import org.gradle.kotlin.dsl.register
import org.gradle.kotlin.dsl.withType

fun Project.nessieConfigureTestTasks() {
  gradle.sharedServices.registerIfAbsent(
    "intTestParallelismConstraint",
    TestingParallelismHelper::class.java
  ) {
    val intTestParallelism =
      Integer.getInteger(
        "nessie.intTestParallelism",
        Math.max(Runtime.getRuntime().availableProcessors() / 4, 1)
      )
    maxParallelUsages.set(intTestParallelism)
  }

  gradle.sharedServices.registerIfAbsent(
    "testParallelismConstraint",
    TestingParallelismHelper::class.java
  ) {
    val intTestParallelism =
      Integer.getInteger(
        "nessie.testParallelism",
        Math.max(Runtime.getRuntime().availableProcessors(), 1)
      )
    maxParallelUsages.set(intTestParallelism)
  }

  tasks.withType<Test>().configureEach {
    useJUnitPlatform {}
    val testJvmArgs: String? by project
    val testHeapSize: String? by project
    if (testJvmArgs != null) {
      jvmArgs((testJvmArgs as String).split(" "))
    }

    systemProperty("file.encoding", "UTF-8")
    systemProperty("user.language", "en")
    systemProperty("user.country", "US")
    systemProperty("user.variant", "")
    systemProperty("test.log.level", testLogLevel())
    environment("TESTCONTAINERS_REUSE_ENABLE", "true")
    filter {
      isFailOnNoMatchingTests = false
      when (name) {
        "test" -> {
          includeTestsMatching("*Test")
          includeTestsMatching("Test*")
          excludeTestsMatching("Abstract*")
          excludeTestsMatching("IT*")
        }
        "intTest" -> includeTestsMatching("IT*")
      }
    }
    if (name != "test") {
      mustRunAfter(tasks.named<Test>("test"))
    } else {
      usesService(
        gradle.sharedServices.registrations.named("testParallelismConstraint").get().service
      )
    }

    if (plugins.hasPlugin("io.quarkus")) {
      jvmArgs("--add-opens=java.base/java.util=ALL-UNNAMED")
      systemProperty("quarkus.log.level", testLogLevel())
      systemProperty("quarkus.log.console.level", testLogLevel())
      systemProperty("http.access.log.level", testLogLevel())

      minHeapSize = if (testHeapSize != null) testHeapSize as String else "512m"
      maxHeapSize = if (testHeapSize != null) testHeapSize as String else "1536m"
    } else if (testHeapSize != null) {
      setMinHeapSize(testHeapSize)
      setMaxHeapSize(testHeapSize)
    }
  }
  val intTest =
    tasks.register<Test>("intTest") {
      group = "verification"
      description = "Runs the integration tests."

      usesService(
        gradle.sharedServices.registrations.named("intTestParallelismConstraint").get().service
      )

      if (plugins.hasPlugin("io.quarkus")) {
        dependsOn(tasks.named("quarkusBuild"))
      }

      systemProperty("nessie.integrationTest", "true")
    }
  tasks.named("check") { dependsOn(intTest) }
}

abstract class TestingParallelismHelper : BuildService<BuildServiceParameters.None> {}

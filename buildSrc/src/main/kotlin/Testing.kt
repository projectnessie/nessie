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

import org.gradle.api.Plugin
import org.gradle.api.Project
import org.gradle.api.attributes.TestSuiteType
import org.gradle.api.component.AdhocComponentWithVariants
import org.gradle.api.plugins.JavaPluginExtension
import org.gradle.api.plugins.JavaTestFixturesPlugin
import org.gradle.api.plugins.JvmTestSuitePlugin
import org.gradle.api.plugins.jvm.JvmTestSuite
import org.gradle.api.services.BuildService
import org.gradle.api.services.BuildServiceParameters
import org.gradle.api.tasks.testing.Test
import org.gradle.kotlin.dsl.apply
import org.gradle.kotlin.dsl.configure
import org.gradle.kotlin.dsl.dependencies
import org.gradle.kotlin.dsl.get
import org.gradle.kotlin.dsl.named
import org.gradle.kotlin.dsl.provideDelegate
import org.gradle.kotlin.dsl.register
import org.gradle.kotlin.dsl.withType
import org.gradle.testing.base.TestingExtension

class NessieTestingPlugin : Plugin<Project> {
  override fun apply(project: Project): Unit =
    project.run {
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
            Math.max(Runtime.getRuntime().availableProcessors() / 2, 1)
          )
        maxParallelUsages.set(intTestParallelism)
      }

      apply<JavaTestFixturesPlugin>()
      // Do not publish test fixtures via Maven. Shared, reusable test code should be published as
      // a separate project to retain dependency information.
      val javaComponent = components["java"] as AdhocComponentWithVariants
      javaComponent.withVariantsFromConfiguration(configurations["testFixturesApiElements"]) {
        skip()
      }
      javaComponent.withVariantsFromConfiguration(configurations["testFixturesRuntimeElements"]) {
        skip()
      }
      if (plugins.hasPlugin("io.quarkus")) {
        // This directory somehow disappears... Maybe some weird Quarkus code.
        tasks.named("quarkusGenerateCodeTests") {
          doFirst { buildDir.resolve("resources/testFixtures").mkdirs() }
        }
        tasks.withType<Test>().configureEach {
          doFirst { buildDir.resolve("resources/testFixtures").mkdirs() }
        }
      }

      apply<JvmTestSuitePlugin>()

      tasks.withType<Test>().configureEach {
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

        if (plugins.hasPlugin("io.quarkus")) {
          jvmArgs("--add-opens=java.base/java.util=ALL-UNNAMED")
          // Log-levels are required to be able to parse the HTTP listen URL
          systemProperty("quarkus.log.level", "INFO")
          systemProperty("quarkus.log.console.level", "INFO")
          systemProperty("http.access.log.level", testLogLevel())

          minHeapSize = if (testHeapSize != null) testHeapSize as String else "512m"
          maxHeapSize = if (testHeapSize != null) testHeapSize as String else "1536m"
        } else if (testHeapSize != null) {
          setMinHeapSize(testHeapSize)
          setMaxHeapSize(testHeapSize)
        }

        filter { isFailOnNoMatchingTests = false }
      }

      configure<TestingExtension> {
        val test =
          suites.named<JvmTestSuite>("test") {
            useJUnitJupiter()

            targets.all {
              testTask.configure {
                usesService(
                  gradle.sharedServices.registrations
                    .named("testParallelismConstraint")
                    .get()
                    .service
                )
              }
            }
          }

        suites.register<JvmTestSuite>("intTest") {
          useJUnitJupiter()

          testType.set(TestSuiteType.INTEGRATION_TEST)

          dependencies { implementation.add(project()) }

          val hasQuarkus = plugins.hasPlugin("io.quarkus")

          targets.all {
            testTask.configure {
              usesService(
                gradle.sharedServices.registrations
                  .named("intTestParallelismConstraint")
                  .get()
                  .service
              )

              if (hasQuarkus) {
                dependsOn(tasks.named("quarkusBuild"))
              }

              shouldRunAfter(test)

              systemProperty("nessie.integrationTest", "true")

              // For Quarkus...
              //
              // io.quarkus.test.junit.IntegrationTestUtil.determineBuildOutputDirectory(java.net.URL)
              // is not smart enough :(
              if (hasQuarkus) {
                systemProperty("build.output.directory", buildDir)
                dependsOn(tasks.named("quarkusBuild"))
              }
            }

            if (hasQuarkus) {
              tasks.named("compileIntTestJava") {
                dependsOn(tasks.named("compileQuarkusTestGeneratedSourcesJava"))
              }
            }

            tasks.named("check") { dependsOn(testTask) }
          }

          if (hasQuarkus) {
            sources { java.srcDirs(tasks.named("quarkusGenerateCodeTests")) }
          }
        }
      }

      // Let the test's implementation config extend testImplementation, so it also inherits the
      // project's "main" implementation dependencies (not just the "api" configuration)
      configurations.named("intTestImplementation") {
        extendsFrom(configurations.getByName("testImplementation"))
      }
      dependencies {
        add(
          "intTestImplementation",
          project.extensions
            .getByType(JavaPluginExtension::class.java)
            .sourceSets
            .getByName("test")
            .output
            .dirs
        )
      }
      configurations.named("intTestRuntimeOnly") {
        extendsFrom(configurations.getByName("testRuntimeOnly"))
      }
    }

  abstract class TestingParallelismHelper : BuildService<BuildServiceParameters.None> {}
}

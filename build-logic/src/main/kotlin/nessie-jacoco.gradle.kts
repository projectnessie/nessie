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

/**
 * Provides a series of code-coverage report tasks:
 *
 * Tasks with the pattern `jacoco<test-suite-name>Report` provide a code-coverage report for tests
 * in the "local" project reporting for the project's sources.
 *
 * Tasks with the pattern `<test-suite-name>CodeCoverageReport` provide a code-coverage report for
 * all tests in the "local" project and dependendent projects, reporting for the project's sources
 * and the dependent project's sources. (Note: this behavior is coded in Gradle's
 * `JacocoReportAggregationPlugin`.
 */
import org.gradle.api.internal.lambdas.SerializableLambdas
import org.gradle.configurationcache.extensions.capitalized
import org.gradle.testing.jacoco.plugins.JacocoPluginExtension
import org.gradle.testing.jacoco.plugins.JacocoTaskExtension
import org.gradle.testing.jacoco.tasks.JacocoCoverageVerification
import org.gradle.testing.jacoco.tasks.JacocoReport

plugins {
  jacoco
  `jacoco-report-aggregation`
  `jvm-test-suite`
}

tasks.withType<JacocoReport>().configureEach {
  reports {
    html.required.set(true)
    xml.required.set(true)
  }
}

jacoco { toolVersion = libsRequiredVersion("jacoco") }

if (plugins.hasPlugin("io.quarkus")) {
  tasks.named("classes").configure { dependsOn(tasks.named("compileQuarkusGeneratedSourcesJava")) }

  tasks.withType<Test>().configureEach {
    extensions.configure(JacocoTaskExtension::class.java) {
      val excluded = excludeClassLoaders
      excludeClassLoaders =
        listOf("*QuarkusClassLoader") + (if (excluded != null) excluded else emptyList())
    }
  }
}

// Add the "project-only" jacoco*Report and jacoco*CoverageVerification in addition to the
// default (the one added by the JacocoPlugin for 'test').
testing {
  suites.withType<JvmTestSuite>().configureEach {
    if ("test" != name) {
      val extension = project.extensions.getByType(JacocoPluginExtension::class.java)
      targets.configureEach {
        val testTaskName = testTask.name
        val reportTask =
          project.tasks.register(
            "jacoco${testTaskName.capitalized()}Report",
            JacocoReport::class.java
          )
        reportTask.configure {
          group = LifecycleBasePlugin.VERIFICATION_GROUP
          description =
            String.format("Generates code coverage report for the %s task.", testTaskName)
          executionData(testTask.get())
          sourceSets(project.extensions.getByType(SourceSetContainer::class.java).getByName("main"))
          // TODO: Change the default location for these reports to follow the convention defined in
          // ReportOutputDirectoryAction
          val reportsDir = extension.reportsDirectory
          reports.all(
            SerializableLambdas.action(
              SerializableLambdas.SerializableAction<ConfigurableReport> {
                // For someone looking for the difference between this and the duplicate code above
                // this one uses the `testTaskProvider` and the `reportTask`. The other just
                // uses the `reportTask`.
                // https://github.com/gradle/gradle/issues/6343
                if (outputType == Report.OutputType.DIRECTORY) {
                  (this as DirectoryReport)
                    .outputLocation
                    .convention(reportsDir.dir(testTaskName + "/" + name))
                } else {
                  (this as SingleFileReport)
                    .outputLocation
                    .convention(reportsDir.file(testTaskName + "/" + reportTask.name + "." + name))
                }
              }
            )
          )
        }

        val verification =
          project.tasks.register(
            "jacoco${testTask.name.capitalized()}CoverageVerification",
            JacocoCoverageVerification::class.java
          )
        verification.configure {
          group = LifecycleBasePlugin.VERIFICATION_GROUP
          description =
            String.format(
              "Verifies code coverage metrics based on specified rules for the %s task.",
              testTask.name
            )
          executionData(testTask.get())
          sourceSets(project.extensions.getByType(SourceSetContainer::class.java).getByName("main"))
        }
      }
    }
  }
}

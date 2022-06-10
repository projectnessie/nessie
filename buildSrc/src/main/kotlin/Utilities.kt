/*
 * Copyright (C) 2022 Dremio
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

import org.gradle.api.JavaVersion
import org.gradle.api.Project
import org.gradle.api.artifacts.Dependency
import org.gradle.api.artifacts.DependencyConstraint
import org.gradle.api.artifacts.ExternalModuleDependency
import org.gradle.api.artifacts.ModuleDependency
import org.gradle.api.artifacts.ProjectDependency
import org.gradle.api.internal.artifacts.dependencies.DefaultProjectDependencyConstraint
import org.gradle.api.tasks.testing.Test
import org.gradle.jvm.toolchain.JavaLanguageVersion
import org.gradle.jvm.toolchain.JavaToolchainService
import org.gradle.kotlin.dsl.DependencyHandlerScope
import org.gradle.kotlin.dsl.add
import org.gradle.kotlin.dsl.exclude
import org.gradle.kotlin.dsl.extra

/**
 * Apply the given `sparkVersion` as a `strictly` version constraint and [withSparkExcludes] on the
 * current [Dependency].
 */
fun ModuleDependency.forSpark(sparkVersion: String): ModuleDependency {
  val dep = this as ExternalModuleDependency
  dep.version { strictly(sparkVersion) }
  return this.withSparkExcludes()
}

/** Apply a bunch of common dependency-exclusion to the current Spark [Dependency]. */
fun ModuleDependency.withSparkExcludes(): ModuleDependency {
  return this.exclude("commons-logging", "commons-logging")
    .exclude("log4j", "log4j")
    .exclude("org.slf4j", "slf4j-log4j12")
    .exclude("org.eclipse.jetty", "jetty-util")
    .exclude("org.apache.avro", "avro")
    .exclude("org.apache.arrow", "arrow-vector")
}

fun DependencyHandlerScope.forScala(scalaVersion: String) {
  // Note: Quarkus contains Scala dependencies since 2.9.0
  add("implementation", "org.scala-lang:scala-library") { version { strictly(scalaVersion) } }
  add("implementation", "org.scala-lang:scala-reflect") { version { strictly(scalaVersion) } }
}

/**
 * Forces all [Test] tasks to use Java 11 for test execution, which is mandatory for tests using
 * Spark.
 */
fun Project.forceJava11ForTests() {
  if (!JavaVersion.current().isJava11) {
    tasks.withType(Test::class.java).configureEach {
      val javaToolchains = project.extensions.findByType(JavaToolchainService::class.java)
      javaLauncher.set(
        javaToolchains!!.launcherFor { languageVersion.set(JavaLanguageVersion.of(11)) }
      )
    }
  }
}

fun Project.dependencyVersion(key: String) = rootProject.extra[key].toString()

fun Project.testLogLevel() = System.getProperty("test.log.level", "WARN")

fun ProjectDependency.testJarCapability() {
  val prj = this.dependencyProject
  capabilities { requireCapability("${prj.rootProject.group}:${prj.name}-tests") }
}

fun DependencyConstraint.testJarCapability() {
  if (this is DefaultProjectDependencyConstraint) {
    this.projectDependency.testJarCapability()
  } else {
    throw java.lang.IllegalStateException(
      "Expected an instance of DefaultProjectDependencyConstraint, but got ${this::class.java}"
    )
  }
}

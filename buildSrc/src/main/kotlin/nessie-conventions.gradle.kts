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

import com.github.vlsi.jandex.JandexExtension
import org.gradle.api.JavaVersion
import org.gradle.api.Project
import org.gradle.api.file.DuplicatesStrategy
import org.gradle.api.plugins.JavaPlugin
import org.gradle.api.plugins.JavaPluginExtension
import org.gradle.api.plugins.scala.ScalaPlugin
import org.gradle.api.publish.PublishingExtension
import org.gradle.api.publish.maven.MavenPublication
import org.gradle.api.tasks.bundling.Jar
import org.gradle.api.tasks.compile.JavaCompile
import org.gradle.api.tasks.javadoc.Javadoc
import org.gradle.api.tasks.scala.ScalaDoc
import org.gradle.api.tasks.testing.Test
import org.gradle.external.javadoc.CoreJavadocOptions
import org.gradle.kotlin.dsl.configure
import org.gradle.kotlin.dsl.findByType
import org.gradle.kotlin.dsl.getByName
import org.gradle.kotlin.dsl.maven
import org.gradle.kotlin.dsl.named
import org.gradle.kotlin.dsl.provideDelegate
import org.gradle.kotlin.dsl.register
import org.gradle.kotlin.dsl.repositories
import org.gradle.kotlin.dsl.withType

plugins {
  id("org.projectnessie.buildsupport.jacoco")
  id("org.projectnessie.buildsupport.jandex")
  id("org.projectnessie.buildsupport.checkstyle")
  id("org.projectnessie.buildsupport.spotless")
  id("org.projectnessie.buildsupport.errorprone")
  id("org.projectnessie.buildsupport.publishing")
  `eclipse`
}

configureJava()

testTasks()

scaladocJar()

fun Project.scaladocJar() {
  plugins.withType<ScalaPlugin>().configureEach {
    val scaladoc = tasks.named<ScalaDoc>("scaladoc")

    val jandexExt = extensions.findByType<JandexExtension>()
    if (jandexExt != null) {
      scaladoc.configure { dependsOn(tasks.named("processJandexIndex")) }
    }

    val scaladocJar =
      tasks.register<Jar>("scaladocJar") {
        dependsOn(scaladoc)
        val baseJar = tasks.getByName<Jar>("jar")
        from(scaladoc.get().destinationDir)
        destinationDirectory.set(baseJar.destinationDirectory)
        archiveClassifier.set("scaladoc")
      }

    tasks.named("assemble") { dependsOn(scaladocJar) }

    configure<PublishingExtension> {
      publications {
        withType(MavenPublication::class.java) {
          if (name == "maven") {
            artifact(scaladocJar)
          }
        }
      }
    }
  }
}

fun Project.testTasks() {
  if (projectDir.resolve("src/test").exists()) {
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

        if (plugins.hasPlugin("io.quarkus")) {
          dependsOn(tasks.named("quarkusBuild"))
        }
      }
    tasks.named("check") { dependsOn(intTest) }
  }
}

fun Project.configureJava() {
  tasks.withType<Jar>().configureEach {
    manifest {
      attributes["Implementation-Title"] = "Nessie ${project.name}"
      attributes["Implementation-Version"] = project.version
      attributes["Implementation-Vendor"] = "Dremio"
    }
    duplicatesStrategy = DuplicatesStrategy.WARN
  }

  repositories {
    mavenCentral { content { excludeVersionByRegex("io[.]delta", ".*", ".*-nessie") } }
    maven("https://storage.googleapis.com/nessie-maven") {
      name = "Nessie Delta custom Repository"
      content { includeVersionByRegex("io[.]delta", ".*", ".*-nessie") }
    }
    if (System.getProperty("withMavenLocal").toBoolean()) {
      mavenLocal()
    }
  }

  tasks.withType<JavaCompile>().configureEach {
    options.encoding = "UTF-8"
    options.compilerArgs.add("-parameters")

    // Required to enable incremental compilation w/ immutables, see
    // https://github.com/immutables/immutables/pull/858 and
    // https://github.com/immutables/immutables/issues/804#issuecomment-487366544
    options.compilerArgs.add("-Aimmutables.gradle.incremental")
  }

  tasks.withType<Javadoc>().configureEach {
    val opt = options as CoreJavadocOptions
    // don't spam log w/ "warning: no @param/@return"
    opt.addStringOption("Xdoclint:-reference", "-quiet")
  }

  plugins.withType<JavaPlugin>().configureEach {
    configure<JavaPluginExtension> {
      withJavadocJar()
      withSourcesJar()
      sourceCompatibility = JavaVersion.VERSION_1_8
      targetCompatibility = JavaVersion.VERSION_1_8
    }
  }
}

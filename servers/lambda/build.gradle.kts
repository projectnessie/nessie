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

import io.quarkus.gradle.tasks.QuarkusBuild

plugins {
  `java-library`
  `maven-publish`
  signing
  id("io.quarkus")
  `nessie-conventions`
}

extra["maven.name"] = "Nessie - Lambda Function"

val quarkusRunner by
  configurations.creating {
    description = "Used to reference the generated runner-jar (either fast-jar or uber-jar)"
  }

dependencies {
  implementation(platform(rootProject))
  implementation(platform(project(":nessie-deps-persist")))
  implementation(platform(project(":nessie-deps-quarkus")))
  implementation(enforcedPlatform("io.quarkus:quarkus-bom"))
  implementation(platform("software.amazon.awssdk:bom"))

  implementation(project(":nessie-quarkus")) { exclude("io.quarkus", "quarkus-smallrye-openapi") }
  implementation("io.quarkus:quarkus-amazon-lambda")
  implementation("io.quarkus:quarkus-amazon-lambda-http")
  implementation("software.amazon.awssdk:apache-client") {
    exclude("commons-logging", "commons-logging")
  }
  implementation("software.amazon.awssdk:netty-nio-client")
  implementation("software.amazon.awssdk:url-connection-client")

  testImplementation(platform(project(":nessie-deps-testing")))
  testImplementation(platform("org.junit:junit-bom"))

  testImplementation("io.quarkus:quarkus-test-amazon-lambda")
  testImplementation("io.quarkus:quarkus-jacoco")

  testImplementation("org.mockito:mockito-core")
  testImplementation("org.assertj:assertj-core")
  testImplementation("org.junit.jupiter:junit-jupiter-api")
  testImplementation("org.junit.jupiter:junit-jupiter-params")
  testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine")
}

java {
  sourceCompatibility = JavaVersion.VERSION_11
  targetCompatibility = JavaVersion.VERSION_11
}

project.extra["quarkus.package.type"] =
  if (withUberJar()) "uber-jar" else if (project.hasProperty("native")) "native" else "fast-jar"

// TODO remove the whole block
quarkus { setFinalName("${project.name}-${project.version}") }

val useDocker = project.hasProperty("docker")

val quarkusBuild by
  tasks.getting(QuarkusBuild::class) {
    inputs.property("quarkus.package.type", project.extra["quarkus.package.type"])
    inputs.property("final.name", quarkus.finalName())
    inputs.property("builder-image", dependencyVersion("quarkus.builder-image"))
    inputs.property("container-build", useDocker)
    if (useDocker) {
      // Use the "docker" profile to just build the Docker container image when the native image's
      // been built
      nativeArgs { "container-build" to true }
    }
    nativeArgs { "builder-image" to dependencyVersion("quarkus.builder-image") }
    doFirst {
      // THIS IS A WORKAROUND! the nativeArgs{} thing above doesn't really work
      System.setProperty("quarkus.native.builder-image", dependencyVersion("quarkus.builder-image"))
      if (useDocker) {
        System.setProperty("quarkus.native.container-build", "true")
      }
    }
  }

tasks.withType<Test>().configureEach {
  enabled = false // TODO project.hasProperty("native")  -- verify that tests work

  jvmArgs("--add-opens=java.base/java.util=ALL-UNNAMED")
  systemProperty("quarkus.log.level", testLogLevel())
  systemProperty("quarkus.log.console.level", testLogLevel())
  systemProperty("http.access.log.level", testLogLevel())
  systemProperty("native.image.path", quarkusBuild.nativeRunner)
  systemProperty("quarkus.container-image.build", useDocker)
  systemProperty("quarkus.smallrye.jwt.enabled", "true")
  // TODO requires adjusting the tests - systemProperty("quarkus.http.test-port", "0") -  set this
  //  property in application.properties

  val testHeapSize: String? by project
  minHeapSize = if (testHeapSize != null) testHeapSize as String else "256m"
  maxHeapSize = if (testHeapSize != null) testHeapSize as String else "1024m"
}

artifacts {
  add(
    quarkusRunner.name,
    if (withUberJar()) quarkusBuild.runnerJar else quarkusBuild.fastJar.resolve("quarkus-run.jar")
  ) {
    builtBy(quarkusBuild)
  }
}

// TODO there are no integration-tests ...
//  tasks.named<Test>("intTest") { filter { excludeTestsMatching("ITNative*") } }

listOf("javadoc", "sourcesJar").forEach { name ->
  tasks.named(name) { dependsOn(tasks.named("compileQuarkusGeneratedSourcesJava")) }
}

listOf("checkstyleTest", "compileTestJava").forEach { name ->
  tasks.named(name) { dependsOn(tasks.named("compileQuarkusTestGeneratedSourcesJava")) }
}

// TODO build zip file

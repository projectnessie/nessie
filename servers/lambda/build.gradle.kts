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
  alias(libs.plugins.quarkus)
  `nessie-conventions`
}

extra["maven.name"] = "Nessie - Lambda Function"

val quarkusRunner by
  configurations.creating {
    description = "Used to reference the generated runner-jar (either fast-jar or uber-jar)"
  }

dependencies {
  implementation(project(":nessie-quarkus")) { exclude("io.quarkus", "quarkus-smallrye-openapi") }

  implementation(enforcedPlatform(libs.quarkus.bom))
  implementation("io.quarkus:quarkus-amazon-lambda")
  implementation("io.quarkus:quarkus-amazon-lambda-http")

  implementation(platform(libs.awssdk.bom))
  implementation(libs.awssdk.apache.client)
  implementation(libs.awssdk.apache.client) { exclude("commons-logging", "commons-logging") }
  implementation(libs.awssdk.netty.nio.client)
  implementation(libs.awssdk.url.connection.client)

  testImplementation("io.quarkus:quarkus-test-amazon-lambda")
  testImplementation("io.quarkus:quarkus-jacoco")

  testImplementation(platform(libs.junit.bom))
  testImplementation(libs.bundles.junit.testing)
  testRuntimeOnly(libs.junit.jupiter.engine)
}

preferJava11()

val useDocker = project.hasProperty("docker")
val packageType = quarkusPackageType()
val quarkusBuilderImage = libs.versions.quarkusBuilderImage.get()

quarkus {
  quarkusBuildProperties.put("quarkus.package.type", packageType)
  quarkusBuildProperties.put("quarkus.native.builder-image", quarkusBuilderImage)
  if (useDocker) {
    quarkusBuildProperties.put("quarkus.native.container-build", "true")
    quarkusBuildProperties.put("quarkus.container-image.build", "true")
  }
}

val quarkusBuild by
  tasks.getting(QuarkusBuild::class) {
    outputs.doNotCacheIf("Do not add huge cache artifacts to build cache") { true }
    inputs.property("quarkus.package.type", packageType)
    inputs.property("final.name", quarkus.finalName())
    inputs.property("container-build", useDocker)
    inputs.property("builder-image", quarkusBuilderImage)
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

  val testHeapSize: String? by project
  minHeapSize = if (testHeapSize != null) testHeapSize as String else "256m"
  maxHeapSize = if (testHeapSize != null) testHeapSize as String else "1024m"
}

artifacts {
  add(
    quarkusRunner.name,
    if (quarkusFatJar()) quarkusBuild.runnerJar else quarkusBuild.fastJar.resolve("quarkus-run.jar")
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

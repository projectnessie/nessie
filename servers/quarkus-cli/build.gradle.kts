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

extra["maven.name"] = "Nessie - Quarkus CLI"

dependencies {
  implementation(project(":nessie-quarkus-common"))
  implementation(project(":nessie-services"))
  implementation(project(":nessie-server-store"))
  implementation(project(":nessie-versioned-persist-adapter"))
  implementation(project(":nessie-versioned-spi"))
  implementation(project(":nessie-versioned-transfer"))
  implementation(project(":nessie-versioned-transfer-proto"))
  implementation(project(":nessie-model"))
  implementation(project(":nessie-versioned-persist-non-transactional"))
  implementation(project(":nessie-versioned-persist-in-memory"))
  implementation(project(":nessie-versioned-persist-dynamodb"))
  implementation(project(":nessie-versioned-persist-mongodb"))
  implementation(project(":nessie-versioned-persist-rocks"))
  implementation(project(":nessie-versioned-persist-transactional"))

  implementation(enforcedPlatform(libs.quarkus.bom))
  implementation(enforcedPlatform(libs.quarkus.amazon.services.bom))
  implementation("io.quarkus:quarkus-picocli")

  implementation(libs.protobuf.java)

  implementation(libs.findbugs.jsr305)
  compileOnly(libs.microprofile.openapi)

  implementation(platform(libs.jackson.bom))
  implementation(libs.jackson.databind)
  implementation(libs.jackson.annotations)

  implementation(libs.agroal.pool)
  implementation(libs.h2)
  implementation(libs.postgresql)

  compileOnly(libs.immutables.builder)
  compileOnly(libs.immutables.value.annotations)
  annotationProcessor(libs.immutables.value.processor)

  testImplementation(project(":nessie-quarkus-tests"))
  testImplementation(project(":nessie-versioned-persist-mongodb-test"))
  testImplementation(project(":nessie-versioned-tests"))
  testImplementation("io.quarkus:quarkus-jacoco")
  testImplementation("io.quarkus:quarkus-junit5")
  testCompileOnly(libs.microprofile.openapi)

  testImplementation(platform(libs.junit.bom))
  testImplementation(libs.bundles.junit.testing)
  testRuntimeOnly(libs.junit.jupiter.engine)
}

preferJava11()

tasks.withType<ProcessResources>().configureEach {
  from("src/main/resources") {
    expand("nessieVersion" to version)
    duplicatesStrategy = DuplicatesStrategy.INCLUDE
  }
}

// nessie-quarkus-cli module needs to be adopted before we can generate a native runner
project.extra["quarkus.package.type"] =
  if (withUberJar() || project.hasProperty("native")) "uber-jar" else "fast-jar"

// TODO remove the whole block
quarkus { setFinalName("${project.name}-${project.version}") }

tasks.withType<QuarkusBuild>().configureEach {
  inputs.property("quarkus.package.type", project.extra["quarkus.package.type"])
  inputs.property("final.name", quarkus.finalName())
  val quarkusBuilderImage = libs.versions.quarkusUbiNativeImage.get()
  inputs.property("builder-image", quarkusBuilderImage)
  nativeArgs { "builder-image" to quarkusBuilderImage }
  doFirst {
    // THIS IS A WORKAROUND! the nativeArgs{} thing above doesn't really work
    System.setProperty("quarkus.native.builder-image", quarkusBuilderImage)
  }
}

if (withUberJar()) {
  afterEvaluate {
    publishing {
      publications {
        named<MavenPublication>("maven") {
          val quarkusBuild = tasks.getByName<QuarkusBuild>("quarkusBuild")
          artifact(quarkusBuild.runnerJar) {
            classifier = "runner"
            builtBy(quarkusBuild)
          }
        }
      }
    }
  }
}

listOf("javadoc", "sourcesJar").forEach { name ->
  tasks.named(name) { dependsOn(tasks.named("compileQuarkusGeneratedSourcesJava")) }
}

listOf("checkstyleTest", "compileTestJava").forEach { name ->
  tasks.named(name) { dependsOn(tasks.named("compileQuarkusTestGeneratedSourcesJava")) }
}

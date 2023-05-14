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
import org.apache.tools.ant.taskdefs.condition.Os

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

  implementation(project(":nessie-versioned-storage-cache"))
  implementation(project(":nessie-versioned-storage-cassandra"))
  implementation(project(":nessie-versioned-storage-common"))
  implementation(project(":nessie-versioned-storage-dynamodb"))
  implementation(project(":nessie-versioned-storage-inmemory"))
  implementation(project(":nessie-versioned-storage-jdbc"))
  implementation(project(":nessie-versioned-storage-mongodb"))
  implementation(project(":nessie-versioned-storage-rocksdb"))
  implementation(project(":nessie-versioned-storage-telemetry"))

  implementation(enforcedPlatform(libs.quarkus.bom))
  implementation(enforcedPlatform(libs.quarkus.amazon.services.bom))
  implementation("io.quarkus:quarkus-picocli")

  // javax/jakarta
  compileOnly(libs.jakarta.annotation.api)
  compileOnly(libs.findbugs.jsr305)

  compileOnly(libs.errorprone.annotations)
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

  testFixturesApi(project(":nessie-quarkus-tests"))
  testFixturesImplementation(project(":nessie-versioned-persist-adapter"))
  intTestImplementation(project(":nessie-versioned-persist-mongodb-test"))
  testFixturesApi(project(":nessie-versioned-tests"))
  intTestImplementation(project(":nessie-versioned-storage-mongodb"))
  testFixturesApi(project(":nessie-versioned-storage-testextension"))
  testFixturesApi(enforcedPlatform(libs.quarkus.bom))
  testFixturesApi("io.quarkus:quarkus-junit5")
  testFixturesApi(libs.microprofile.openapi)

  testFixturesApi(platform(libs.junit.bom))
  testFixturesApi(libs.bundles.junit.testing)
}

buildForJava11()

tasks.withType<ProcessResources>().configureEach {
  from("src/main/resources") {
    expand("nessieVersion" to version)
    duplicatesStrategy = DuplicatesStrategy.INCLUDE
  }
}

// nessie-quarkus-cli module needs to be adopted before we can generate a native runner
val packageType = quarkusNonNativePackageType()

quarkus { quarkusBuildProperties.put("quarkus.package.type", packageType) }

tasks.withType<QuarkusBuild>().configureEach {
  outputs.doNotCacheIf("Do not add huge cache artifacts to build cache") { true }
  inputs.property("final.name", quarkus.finalName())
  inputs.properties(quarkus.quarkusBuildProperties.get())
}

if (quarkusFatJar()) {
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

// Testcontainers is not supported on Windows :(
if (Os.isFamily(Os.FAMILY_WINDOWS)) {
  tasks.named<Test>("intTest") { this.enabled = false }
}

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

extra["maven.name"] = "Nessie - Quarkus CLI"

dependencies {
  compileOnly(platform(project(":nessie-deps-build-only")))
  implementation(platform(rootProject))
  implementation(platform(project(":nessie-deps-persist")))
  implementation(platform(project(":nessie-deps-quarkus")))
  annotationProcessor(platform(project(":nessie-deps-build-only")))
  implementation(enforcedPlatform("io.quarkus:quarkus-bom"))
  implementation(enforcedPlatform("io.quarkus.platform:quarkus-amazon-services-bom"))
  implementation(platform("com.fasterxml.jackson:jackson-bom"))

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

  implementation("io.quarkus:quarkus-picocli")

  implementation("com.google.protobuf:protobuf-java")

  implementation("com.google.code.findbugs:jsr305")
  compileOnly("org.eclipse.microprofile.openapi:microprofile-openapi-api")

  implementation("com.fasterxml.jackson.core:jackson-databind")
  implementation("com.fasterxml.jackson.core:jackson-annotations")

  implementation("io.agroal:agroal-pool")
  implementation("com.h2database:h2")
  implementation("org.postgresql:postgresql")

  compileOnly("org.immutables:builder")
  compileOnly("org.immutables:value-annotations")
  annotationProcessor("org.immutables:value-processor")

  testImplementation(platform(project(":nessie-deps-testing")))
  testImplementation(platform("org.junit:junit-bom"))

  testImplementation(project(":nessie-quarkus-tests"))
  testImplementation(project(":nessie-versioned-persist-mongodb-test"))
  testImplementation(project(":nessie-versioned-tests"))
  testImplementation("io.quarkus:quarkus-jacoco")
  testImplementation("io.quarkus:quarkus-junit5")
  testCompileOnly("org.eclipse.microprofile.openapi:microprofile-openapi-api")

  testImplementation("org.assertj:assertj-core")
  testImplementation("org.mockito:mockito-core")
  testImplementation("org.junit.jupiter:junit-jupiter-api")
  testImplementation("org.junit.jupiter:junit-jupiter-params")
  testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine")
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
  inputs.property("builder-image", dependencyVersion("quarkus.builder-image"))
  nativeArgs { "builder-image" to dependencyVersion("quarkus.builder-image") }
  doFirst {
    // THIS IS A WORKAROUND! the nativeArgs{} thing above doesn't really work
    System.setProperty("quarkus.native.builder-image", dependencyVersion("quarkus.builder-image"))
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

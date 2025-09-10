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

import org.apache.tools.ant.taskdefs.condition.Os

plugins { id("nessie-conventions-java11") }

publishingHelper { mavenName = "Nessie - Import/Export" }

sourceSets {
  // This implicitly also adds all required Gradle tasks and configurations.
  register("relatedObjects")
}

dependencies {
  implementation(project(":nessie-model"))
  implementation(project(":nessie-versioned-spi"))
  implementation(project(":nessie-versioned-transfer-proto"))
  implementation(project(":nessie-versioned-transfer-related"))
  implementation(project(":nessie-versioned-storage-batching"))
  implementation(project(":nessie-versioned-storage-common"))
  implementation(project(":nessie-versioned-storage-common-serialize"))
  implementation(project(":nessie-versioned-storage-store"))
  runtimeOnly(project(":nessie-catalog-service-transfer"))

  implementation(platform(libs.jackson.bom))
  implementation("com.fasterxml.jackson.core:jackson-databind")
  implementation("com.fasterxml.jackson.core:jackson-annotations")

  compileOnly(libs.microprofile.openapi)
  compileOnly(libs.errorprone.annotations)
  implementation(libs.guava)
  implementation(libs.agrona)

  compileOnly(libs.jakarta.validation.api)
  compileOnly(libs.jakarta.annotation.api)

  compileOnly(project(":nessie-immutables-std"))
  annotationProcessor(project(":nessie-immutables-std", configuration = "processor"))

  testFixturesImplementation(platform(libs.jackson.bom))
  testFixturesImplementation("com.fasterxml.jackson.core:jackson-databind")
  testFixturesImplementation(libs.guava)
  testFixturesImplementation(libs.logback.classic)
  testFixturesApi(libs.jakarta.annotation.api)
  testFixturesApi(project(":nessie-client"))
  testFixturesApi(project(":nessie-server-store"))
  testFixturesApi(project(":nessie-versioned-transfer-proto"))
  testFixturesApi(project(":nessie-versioned-spi"))

  testFixturesApi(project(":nessie-versioned-storage-cache"))
  testFixturesApi(project(":nessie-versioned-storage-common"))
  testFixturesApi(project(":nessie-versioned-storage-inmemory"))
  testFixturesApi(project(":nessie-versioned-storage-testextension"))
  intTestImplementation(project(":nessie-versioned-storage-cassandra"))
  intTestImplementation(project(":nessie-versioned-storage-cassandra2"))
  intTestImplementation(project(":nessie-versioned-storage-dynamodb"))
  intTestImplementation(project(":nessie-versioned-storage-dynamodb2"))
  intTestImplementation(project(":nessie-versioned-storage-jdbc"))
  intTestImplementation(project(":nessie-versioned-storage-jdbc2"))
  intTestImplementation(project(":nessie-versioned-storage-mongodb"))
  intTestImplementation(project(":nessie-versioned-storage-rocksdb"))

  intTestRuntimeOnly(libs.h2)

  testCompileOnly(libs.microprofile.openapi)

  testFixturesImplementation(libs.jakarta.annotation.api)

  testFixturesImplementation(libs.microprofile.openapi)

  testFixturesApi(platform(libs.junit.bom))
  testFixturesApi(libs.bundles.junit.testing)

  add("relatedObjectsCompileOnly", project(":nessie-versioned-transfer-related"))
  add("relatedObjectsCompileOnly", project(":nessie-versioned-storage-common"))
  add("relatedObjectsCompileOnly", project(":nessie-model"))
  add("relatedObjectsCompileOnly", libs.microprofile.openapi)
  add("relatedObjectsCompileOnly", platform(libs.jackson.bom))
  add("relatedObjectsCompileOnly", "com.fasterxml.jackson.core:jackson-annotations")
}

// Issue w/ testcontainers/podman in GH workflows :(
if (Os.isFamily(Os.FAMILY_MAC) && System.getenv("CI") != null) {
  tasks.named<Test>("intTest").configure { this.enabled = false }
}

val relatedObjectsJar by
  tasks.registering(Jar::class) {
    group = "build"
    archiveBaseName = "related-objects-for-tests"
    from(tasks.named("compileRelatedObjectsJava"), tasks.named("processRelatedObjectsResources"))
  }

tasks.named("test", Test::class.java) {
  dependsOn(relatedObjectsJar)
  systemProperty(
    "related-objects-jar",
    relatedObjectsJar
      .get()
      .archiveFile
      .get()
      .asFile
      .relativeTo(project.layout.projectDirectory.asFile),
  )
}

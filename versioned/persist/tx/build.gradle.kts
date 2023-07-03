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

plugins {
  id("nessie-conventions-server8")
  id("nessie-jacoco")
}

extra["maven.name"] = "Nessie - Versioned - Persist - Transactional"

dependencies {
  implementation(project(":nessie-model"))
  implementation(project(":nessie-versioned-persist-adapter"))
  implementation(project(":nessie-versioned-persist-serialize"))
  implementation(project(":nessie-versioned-spi"))
  implementation(libs.guava)
  compileOnly(libs.immutables.value.annotations)
  annotationProcessor(libs.immutables.value.processor)
  implementation(libs.slf4j.api)

  // javax/jakarta
  compileOnly(libs.jakarta.annotation.api)
  compileOnly(libs.findbugs.jsr305)

  compileOnly(libs.agroal.pool)
  compileOnly(libs.h2)
  compileOnly(libs.postgresql)

  testFixturesApi(project(":nessie-versioned-persist-adapter"))
  testFixturesApi(project(":nessie-versioned-persist-serialize"))
  testFixturesApi(project(":nessie-versioned-spi"))

  testFixturesApi(project(":nessie-versioned-tests"))
  testFixturesCompileOnly(libs.immutables.value.annotations)
  testFixturesAnnotationProcessor(libs.immutables.value.processor)
  testFixturesApi(project(":nessie-versioned-persist-testextension"))
  testFixturesApi(project(":nessie-versioned-persist-tests"))
  testFixturesApi(project(":nessie-versioned-persist-transactional-test"))
  testFixturesImplementation(libs.logback.classic)
  testRuntimeOnly(libs.h2)
  intTestRuntimeOnly(libs.postgresql)

  testFixturesImplementation(platform(libs.junit.bom))
  testFixturesImplementation(libs.bundles.junit.testing)
}

tasks.named<Test>("intTest").configure {
  systemProperty("it.nessie.dbs", System.getProperty("it.nessie.dbs", "postgres"))
  systemProperty(
    "it.nessie.container.postgres.tag",
    System.getProperty("it.nessie.container.postgres.tag", libs.versions.postgresContainerTag.get())
  )
}

// Testcontainers is not supported on Windows :(
if (Os.isFamily(Os.FAMILY_WINDOWS)) {
  tasks.named<Test>("intTest").configure { this.enabled = false }
}

// Issue w/ testcontainers/podman in GH workflows :(
if (Os.isFamily(Os.FAMILY_MAC) && System.getenv("CI") != null) {
  tasks.named<Test>("intTest").configure { this.enabled = false }
}

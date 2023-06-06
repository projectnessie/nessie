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

import com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar

plugins {
  id("nessie-conventions-server")
  id("nessie-shadow-jar")
}

dependencies {
  implementation(project(":nessie-model"))
  implementation(project(":nessie-versioned-tests"))
  implementation(project(":nessie-versioned-spi"))
  implementation(project(":nessie-versioned-persist-adapter"))
  implementation(project(":nessie-versioned-persist-store"))
  implementation(libs.jmh.core)
  annotationProcessor(libs.jmh.generator.annprocess)

  compileOnly(platform(libs.jackson.bom))
  compileOnly("com.fasterxml.jackson.core:jackson-annotations")

  compileOnly(libs.microprofile.openapi)

  implementation(project(":nessie-versioned-persist-in-memory"))
  implementation(project(":nessie-versioned-persist-in-memory-test"))
  implementation(project(":nessie-versioned-persist-rocks"))
  implementation(project(":nessie-versioned-persist-rocks-test"))
  implementation(project(":nessie-versioned-persist-dynamodb"))
  implementation(project(":nessie-versioned-persist-dynamodb-test"))
  implementation(project(":nessie-versioned-persist-mongodb"))
  implementation(project(":nessie-versioned-persist-mongodb-test"))
  implementation(project(":nessie-versioned-persist-transactional"))
  implementation(project(":nessie-versioned-persist-transactional-test"))
  implementation(project(":nessie-versioned-persist-testextension"))
  implementation(libs.agroal.pool)
  implementation(libs.h2)
  implementation(libs.postgresql)
}

val shadowJar =
  tasks.named<ShadowJar>("shadowJar") {
    manifest { attributes["Main-Class"] = "org.openjdk.jmh.Main" }
  }

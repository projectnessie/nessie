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
  `java-library`
  `maven-publish`
  signing
  id("com.github.johnrengelman.shadow")
  `nessie-conventions`
}

dependencies {
  implementation(platform(rootProject))
  implementation(platform(project(":nessie-deps-persist")))
  implementation(platform(project(":nessie-deps-testing")))
  annotationProcessor(platform(project(":nessie-deps-build-only")))
  compileOnly(platform("com.fasterxml.jackson:jackson-bom"))

  implementation(project(":nessie-model"))
  implementation(project(":nessie-versioned-tests"))
  implementation(project(":nessie-versioned-spi"))
  implementation(project(":nessie-versioned-persist-adapter"))
  implementation(project(":nessie-versioned-persist-store"))
  implementation(project(":nessie-versioned-persist-tests"))
  implementation("org.openjdk.jmh:jmh-core")
  annotationProcessor("org.openjdk.jmh:jmh-generator-annprocess")
  compileOnly("com.fasterxml.jackson.core:jackson-annotations")
  compileOnly("org.eclipse.microprofile.openapi:microprofile-openapi-api")

  implementation(project(":nessie-versioned-persist-in-memory"))
  implementation(project(":nessie-versioned-persist-in-memory")) { testJarCapability() }
  implementation(project(":nessie-versioned-persist-rocks"))
  implementation(project(":nessie-versioned-persist-rocks")) { testJarCapability() }
  implementation(project(":nessie-versioned-persist-dynamodb"))
  implementation(project(":nessie-versioned-persist-dynamodb")) { testJarCapability() }
  implementation(project(":nessie-versioned-persist-mongodb"))
  implementation(project(":nessie-versioned-persist-mongodb")) { testJarCapability() }
  implementation(project(":nessie-versioned-persist-transactional"))
  implementation(project(":nessie-versioned-persist-transactional")) { testJarCapability() }
  implementation("io.agroal:agroal-pool")
  implementation("com.h2database:h2")
  implementation("org.postgresql:postgresql")
}

val shadowJar =
  tasks.named<ShadowJar>("shadowJar") {
    manifest { attributes["Main-Class"] = "org.openjdk.jmh.Main" }
  }

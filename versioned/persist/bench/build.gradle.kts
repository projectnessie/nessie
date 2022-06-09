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
  jacoco
  `maven-publish`
  id("com.github.johnrengelman.shadow")
  id("org.projectnessie.nessie-project")
}

dependencies {
  implementation(platform(rootProject))
  annotationProcessor(platform(rootProject))
  implementation(projects.versioned.tests)
  implementation(projects.versioned.spi)
  implementation(projects.versioned.persist.adapter)
  implementation(projects.versioned.persist.persistStore)
  implementation(projects.versioned.persist.persistTests)
  implementation("org.openjdk.jmh:jmh-core")
  annotationProcessor("org.openjdk.jmh:jmh-generator-annprocess")

  implementation(projects.versioned.persist.inmem)
  implementation(projects.versioned.persist.inmem) { testJarCapability() }
  implementation(projects.versioned.persist.rocks)
  implementation(projects.versioned.persist.rocks) { testJarCapability() }
  implementation(projects.versioned.persist.dynamodb)
  implementation(projects.versioned.persist.dynamodb) { testJarCapability() }
  implementation(projects.versioned.persist.mongodb)
  implementation(projects.versioned.persist.mongodb) { testJarCapability() }
  implementation(projects.versioned.persist.tx)
  implementation(projects.versioned.persist.tx) { testJarCapability() }
  implementation("io.agroal:agroal-pool")
  implementation("com.h2database:h2")
  implementation("org.postgresql:postgresql")
}

val shadowJar =
  tasks.named<ShadowJar>("shadowJar") {
    manifest { attributes["Main-Class"] = "org.openjdk.jmh.Main" }
  }

/*
 * Copyright (C) 2024 Dremio
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
  id("com.gradleup.shadow")
  id("nessie-conventions-java11")
  alias(libs.plugins.jmh)
}

description = "Nessie - Catalog - Iceberg table format - Microbenchmarks"

val versionIceberg = libs.versions.iceberg.get()

dependencies {
  jmhImplementation(project(":nessie-catalog-files-api"))
  jmhImplementation(project(":nessie-catalog-files-impl"))
  jmhImplementation(project(":nessie-catalog-format-iceberg"))
  jmhImplementation(testFixtures(project(":nessie-catalog-format-iceberg")))
  jmhImplementation(project(":nessie-catalog-format-iceberg-fixturegen"))
  jmhImplementation(project(":nessie-catalog-model"))
  jmhImplementation(project(":nessie-catalog-service-impl"))
  jmhImplementation(project(":nessie-tasks-api"))
  jmhImplementation(project(":nessie-versioned-storage-common"))
  jmhImplementation(project(":nessie-versioned-storage-common-serialize"))
  jmhImplementation("org.apache.iceberg:iceberg-core:$versionIceberg")

  jmhImplementation(platform(libs.jackson.bom))
  jmhImplementation("com.fasterxml.jackson.core:jackson-annotations")
  jmhImplementation("com.fasterxml.jackson.datatype:jackson-datatype-jsr310")

  jmhImplementation(libs.guava)
  jmhImplementation(libs.avro)
}

jmh { jmhVersion.set(libs.versions.jmh.get()) }

tasks.named<ShadowJar>("jmhJar").configure {
  outputs.cacheIf { false } // do not cache uber/shaded jars
  exclude("META-INF/jandex.idx")
  mergeServiceFiles()
}

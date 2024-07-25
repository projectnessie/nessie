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

plugins { id("nessie-conventions-quarkus") }

publishingHelper { mavenName = "Nessie - Quarkus Dist-Cache" }

// Need to use :nessie-model-quarkus instead of :nessie-model here, because Quarkus w/
// resteasy-reactive does not work well with multi-release jars, but as long as we support Java 8
// for clients, we have to live with :nessie-model producing an MR-jar. See
// https://github.com/quarkusio/quarkus/issues/40236 and
// https://github.com/projectnessie/nessie/issues/8390.
configurations.all { exclude(group = "org.projectnessie.nessie", module = "nessie-model") }

dependencies {
  implementation(project(":nessie-quarkus-config"))
  implementation(project(":nessie-quarkus-common"))
  implementation(project(":nessie-versioned-storage-cache"))
  implementation(project(":nessie-versioned-storage-common"))
  implementation(project(":nessie-network-tools"))

  implementation(enforcedPlatform(libs.quarkus.bom))
  implementation("io.quarkus:quarkus-core")
  implementation("io.quarkus:quarkus-resteasy-reactive")

  implementation("com.fasterxml.jackson.core:jackson-databind")
  compileOnly("com.fasterxml.jackson.core:jackson-annotations")

  implementation(libs.guava)
  implementation(libs.slf4j.api)

  compileOnly(libs.immutables.value.annotations)
  annotationProcessor(libs.immutables.value.processor)

  testFixturesApi(platform(libs.junit.bom))
  testFixturesApi(libs.bundles.junit.testing)
}

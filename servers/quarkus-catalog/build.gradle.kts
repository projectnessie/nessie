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

publishingHelper { mavenName = "Nessie - Quarkus Catalog" }

// Need to use :nessie-model-quarkus instead of :nessie-model here, because Quarkus w/
// resteasy-reactive does not work well with multi-release jars, but as long as we support Java 8
// for clients, we have to live with :nessie-model producing an MR-jar. See
// https://github.com/quarkusio/quarkus/issues/40236 and
// https://github.com/projectnessie/nessie/issues/8390.
configurations.all { exclude(group = "org.projectnessie.nessie", module = "nessie-model") }

dependencies {
  implementation(project(":nessie-combined-cs"))
  implementation(project(":nessie-rest-services"))
  implementation(project(":nessie-quarkus-config"))
  implementation(project(":nessie-catalog-files-api"))
  implementation(project(":nessie-catalog-files-impl"))
  implementation(project(":nessie-catalog-secrets-api"))
  implementation(project(":nessie-catalog-service-common"))
  implementation(project(":nessie-catalog-service-impl"))
  implementation(project(":nessie-catalog-service-rest"))
  implementation(project(":nessie-tasks-api"))
  implementation(project(":nessie-tasks-service-async"))
  implementation(project(":nessie-tasks-service-impl"))
  implementation(project(":nessie-services-config"))
  implementation(project(":nessie-versioned-storage-common"))

  implementation(enforcedPlatform(libs.quarkus.bom))
  implementation(enforcedPlatform(libs.quarkus.amazon.services.bom))
  implementation("io.quarkus:quarkus-core")
  implementation("io.quarkus:quarkus-jackson")
  implementation("io.micrometer:micrometer-core")
  implementation("io.smallrye.config:smallrye-config-core")
  implementation("io.smallrye:smallrye-context-propagation")
  implementation(
    "org.eclipse.microprofile.context-propagation:microprofile-context-propagation-api"
  )
  implementation("org.eclipse.microprofile.health:microprofile-health-api")

  implementation(libs.jakarta.ws.rs.api)

  implementation(platform(libs.awssdk.bom))
  implementation("software.amazon.awssdk:s3")
  implementation("software.amazon.awssdk:sts")
  implementation("software.amazon.awssdk:apache-client") {
    exclude("commons-logging", "commons-logging")
  }

  implementation(platform(libs.google.cloud.storage.bom))
  implementation("com.google.cloud:google-cloud-storage")

  implementation(platform(libs.azuresdk.bom))
  implementation("com.azure:azure-storage-file-datalake")
  implementation("com.azure:azure-identity")

  implementation(libs.guava)

  testFixturesApi(platform(libs.junit.bom))
  testFixturesApi(libs.bundles.junit.testing)

  testFixturesApi(enforcedPlatform(libs.quarkus.bom))
  testFixturesApi("io.quarkus:quarkus-core")

  testRuntimeOnly(libs.logback.classic)
}

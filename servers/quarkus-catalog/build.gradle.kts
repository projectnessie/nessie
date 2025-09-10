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

plugins { id("nessie-conventions-java21") }

publishingHelper { mavenName = "Nessie - Quarkus Catalog" }

dependencies {
  implementation(project(":nessie-rest-services"))
  implementation(project(":nessie-quarkus-config"))
  implementation(project(":nessie-catalog-files-api"))
  implementation(project(":nessie-catalog-files-impl"))
  implementation(project(":nessie-catalog-secrets-api"))
  implementation(project(":nessie-catalog-service-common"))
  implementation(project(":nessie-catalog-service-config"))
  implementation(project(":nessie-catalog-service-impl"))
  implementation(project(":nessie-catalog-service-rest"))
  implementation(project(":nessie-tasks-api"))
  implementation(project(":nessie-tasks-service-async"))
  implementation(project(":nessie-tasks-service-impl"))
  implementation(project(":nessie-services-config"))
  implementation(project(":nessie-versioned-storage-common"))

  implementation(quarkusPlatform(project))
  implementation(quarkusExtension(project, "amazon-services"))
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

  testFixturesApi(quarkusPlatform(project))
  testFixturesApi("io.quarkus:quarkus-core")

  testRuntimeOnly(libs.logback.classic)
}

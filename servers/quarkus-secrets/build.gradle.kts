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

plugins { id("nessie-conventions-java21") }

publishingHelper { mavenName = "Nessie - Quarkus Secrets" }

// Need to use :nessie-model-quarkus instead of :nessie-model here, because Quarkus w/
// resteasy-reactive does not work well with multi-release jars, but as long as we support Java 8
// for clients, we have to live with :nessie-model producing an MR-jar. See
// https://github.com/quarkusio/quarkus/issues/40236 and
// https://github.com/projectnessie/nessie/issues/8390.
configurations.all { exclude(group = "org.projectnessie.nessie", module = "nessie-model") }

dependencies {
  implementation(project(":nessie-catalog-secrets-api"))
  implementation(project(":nessie-catalog-secrets-smallrye"))
  implementation(project(":nessie-catalog-secrets-cache"))
  implementation(project(":nessie-catalog-secrets-aws"))
  implementation(project(":nessie-catalog-secrets-gcs"))
  implementation(project(":nessie-catalog-secrets-azure"))
  implementation(project(":nessie-catalog-secrets-vault"))
  implementation(project(":nessie-quarkus-config"))

  implementation(quarkusPlatform(project))
  implementation(quarkusExtension(project, "amazon-services"))
  implementation("io.quarkus:quarkus-core")
  implementation("io.quarkus:quarkus-jackson")
  implementation("io.micrometer:micrometer-core")

  implementation(platform(libs.awssdk.bom))
  implementation("software.amazon.awssdk:s3")
  implementation("software.amazon.awssdk:sts")
  implementation("software.amazon.awssdk:apache-client") {
    exclude("commons-logging", "commons-logging")
  }

  implementation(quarkusExtension(project, "amazon-services"))
  implementation("io.quarkiverse.amazonservices:quarkus-amazon-secretsmanager")

  implementation(quarkusExtension(project, "google-cloud-services"))
  implementation("io.quarkiverse.googlecloudservices:quarkus-google-cloud-secret-manager")

  implementation(libs.quarkus.vault)

  implementation(enforcedPlatform(libs.quarkus.azure.services.bom))
  implementation("io.quarkiverse.azureservices:quarkus-azure-keyvault")

  implementation(platform(libs.azuresdk.bom))
  implementation("com.azure:azure-identity")

  implementation(libs.guava)

  testFixturesApi(platform(libs.junit.bom))
  testFixturesApi(libs.bundles.junit.testing)

  testFixturesApi(quarkusPlatform(project))
  testFixturesApi("io.quarkus:quarkus-core")

  testRuntimeOnly(libs.logback.classic)
}

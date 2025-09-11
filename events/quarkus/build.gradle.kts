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

plugins {
  alias(libs.plugins.quarkus)
    .version(
      libs.plugins.quarkus.asProvider().map {
        System.getProperty("quarkus.custom.version", it.version.requiredVersion)
      }
    )
  id("nessie-conventions-quarkus")
}

publishingHelper { mavenName = "Nessie - Events - Quarkus" }

cassandraDriverTweak()

dependencies {
  implementation(project(":nessie-versioned-spi"))
  implementation(project(":nessie-events-api"))
  implementation(project(":nessie-events-spi"))
  implementation(project(":nessie-events-service"))
  implementation(project(":nessie-quarkus-config"))

  // Quarkus
  implementation(quarkusPlatform(project))
  implementation("io.quarkus:quarkus-vertx")

  // Metrics
  implementation("io.micrometer:micrometer-core")

  // OpenTelemetry
  implementation("io.opentelemetry:opentelemetry-api")

  // Jackson
  compileOnly("com.fasterxml.jackson.core:jackson-annotations")

  testImplementation(project(":nessie-model"))

  testImplementation(quarkusPlatform(project))
  testImplementation("io.quarkus:quarkus-opentelemetry")
  testImplementation("io.quarkus:quarkus-micrometer")
  testImplementation("io.quarkus:quarkus-micrometer-registry-prometheus")
  testImplementation("io.quarkus:quarkus-junit5")
  testImplementation("io.quarkus:quarkus-junit5-mockito")

  testImplementation("io.opentelemetry:opentelemetry-sdk-trace")

  testImplementation(platform(libs.junit.bom))
  testImplementation(libs.bundles.junit.testing)
  testImplementation(libs.awaitility)

  testCompileOnly("com.fasterxml.jackson.core:jackson-annotations")
  testCompileOnly(libs.microprofile.openapi)
}

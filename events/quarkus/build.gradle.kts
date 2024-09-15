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
  id("nessie-conventions-quarkus")
}

publishingHelper { mavenName = "Nessie - Events - Quarkus" }

dependencies {
  implementation(project(":nessie-model"))
  implementation(project(":nessie-catalog-model"))
  implementation(project(":nessie-versioned-spi"))
  implementation(project(":nessie-events-api"))
  implementation(project(":nessie-events-spi"))
  implementation(project(":nessie-events-service"))
  implementation(project(":nessie-quarkus-config"))

  compileOnly(libs.microprofile.openapi)
  compileOnly(platform(libs.jackson.bom))
  compileOnly("com.fasterxml.jackson.core:jackson-annotations")

  // Quarkus
  implementation(enforcedPlatform(libs.quarkus.bom))
  implementation("io.quarkus:quarkus-vertx")

  // Metrics
  implementation("io.micrometer:micrometer-core")

  // OpenTelemetry
  implementation("io.opentelemetry:opentelemetry-api")

  testImplementation(project(":nessie-model"))

  testImplementation(enforcedPlatform(libs.quarkus.bom))
  testImplementation("io.quarkus:quarkus-opentelemetry")
  testImplementation("io.quarkus:quarkus-micrometer")
  testImplementation("io.quarkus:quarkus-micrometer-registry-prometheus")
  testImplementation("io.quarkus:quarkus-junit5")
  testImplementation("io.quarkus:quarkus-junit5-mockito")

  testImplementation("io.opentelemetry:opentelemetry-sdk-trace")

  testImplementation(platform(libs.junit.bom))
  testImplementation(libs.bundles.junit.testing)
  testImplementation(libs.awaitility)

  testCompileOnly(platform(libs.jackson.bom))
  testCompileOnly("com.fasterxml.jackson.core:jackson-annotations")
  testCompileOnly(libs.microprofile.openapi)
}

listOf("javadoc", "sourcesJar").forEach { name ->
  tasks.named(name).configure { dependsOn("compileQuarkusGeneratedSourcesJava") }
}

listOf("checkstyleTest", "compileTestJava").forEach { name ->
  tasks.named(name).configure { dependsOn("compileQuarkusTestGeneratedSourcesJava") }
}

tasks.named("quarkusDependenciesBuild").configure { dependsOn("processJandexIndex") }

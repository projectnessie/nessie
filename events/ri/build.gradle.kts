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

import org.apache.tools.ant.taskdefs.condition.Os

plugins {
  alias(libs.plugins.quarkus)
    .version(
      libs.plugins.quarkus.asProvider().map {
        System.getProperty("quarkus.custom.version", it.version.requiredVersion)
      }
    )
  id("nessie-conventions-quarkus")
}

extra["maven.name"] = "Nessie - Events - SPI Reference Implementation"

cassandraDriverTweak()

dependencies {
  implementation(project(":nessie-model"))
  implementation(project(":nessie-events-api"))
  implementation(project(":nessie-events-spi"))

  // Quarkus
  implementation(quarkusPlatform(project))
  implementation("io.quarkus:quarkus-core")

  // Quarkus - Kafka
  implementation("io.quarkus:quarkus-messaging-kafka")

  // Quarkus - NATS
  implementation(
    "io.quarkiverse.reactivemessaging.nats-jetstream:quarkus-messaging-nats-jetstream:3.23.0"
  )

  // Avro serialization
  implementation(libs.avro)
  implementation("io.quarkus:quarkus-apicurio-registry-avro")
  // This example uses the Apicurio registry, but you can switch to the Confluent registry:
  // implementation("io.quarkus:quarkus-confluent-registry-avro")
  // implementation("io.confluent:kafka-avro-serializer")

  // Jackson serialization
  implementation(platform(libs.jackson.bom))
  implementation("com.fasterxml.jackson.core:jackson-databind")
  implementation("com.fasterxml.jackson.core:jackson-annotations")
  implementation("com.fasterxml.jackson.datatype:jackson-datatype-jdk8")

  compileOnly(libs.microprofile.openapi)

  testImplementation(platform(libs.junit.bom))
  testImplementation(libs.bundles.junit.testing)
  testImplementation(libs.awaitility)
  testImplementation(quarkusPlatform(project))
  testImplementation("io.quarkus:quarkus-junit5")

  testCompileOnly(libs.microprofile.openapi)
  testCompileOnly(libs.immutables.value.annotations)
}

tasks.withType<Checkstyle> { exclude("**/generated/**") }

// Issue w/ testcontainers/podman in GH workflows :(
if (Os.isFamily(Os.FAMILY_MAC) && System.getenv("CI") != null) {
  tasks.withType<Test>().configureEach { this.enabled = false }
}

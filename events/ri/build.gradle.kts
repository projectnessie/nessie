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
  alias(libs.plugins.avro)
  id("nessie-conventions-server")
}

extra["maven.name"] = "Nessie - Events - SPI Reference Implementation"

dependencies {
  implementation(project(":nessie-events-api"))
  implementation(project(":nessie-events-spi"))

  implementation(libs.slf4j.api)
  implementation(libs.kafka.clients)
  implementation(platform(libs.jackson.bom)) // avro pulls in an older version of jackson
  implementation("org.apache.avro:avro:1.11.2")

  testImplementation(platform(libs.junit.bom))
  testImplementation(libs.bundles.junit.testing)
  testImplementation(libs.kafka.streams.test.utils)
  testImplementation(libs.logback.classic)
  testImplementation("io.confluent:kafka-avro-serializer:7.4.0")

  intTestImplementation(platform(libs.testcontainers.bom))
  intTestImplementation("org.testcontainers:junit-jupiter")
  intTestImplementation("org.testcontainers:kafka")
}

tasks.withType<Checkstyle> { exclude("com/example/**/generated/**") }

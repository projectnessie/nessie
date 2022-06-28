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
  `java-library`
  jacoco
  `maven-publish`
  `nessie-conventions`
  id("org.projectnessie.buildsupport.attach-test-jar")
}

extra["maven.name"] = "Nessie - Versioned Store SPI"

dependencies {
  compileOnly(platform(rootProject))
  annotationProcessor(platform(rootProject))
  implementation(platform(rootProject))

  implementation("com.google.protobuf:protobuf-java")
  compileOnly("org.immutables:builder")
  compileOnly("org.immutables:value-annotations")
  annotationProcessor("org.immutables:value-processor")
  compileOnly("jakarta.validation:jakarta.validation-api")
  implementation("com.google.guava:guava")
  implementation("com.google.code.findbugs:jsr305")

  testImplementation(platform(rootProject))

  testImplementation("org.assertj:assertj-core")
  testImplementation("org.mockito:mockito-core")
  testImplementation(platform("org.junit:junit-bom"))
  testImplementation("org.junit.jupiter:junit-jupiter-api")
  testImplementation("org.junit.jupiter:junit-jupiter-params")
  testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine")

  // Need a few things from Quarkus, but don't leak the dependencies
  compileOnly(platform("io.quarkus:quarkus-bom"))
  compileOnly("io.opentracing:opentracing-api")
  compileOnly("io.opentracing:opentracing-util")
  compileOnly("io.micrometer:micrometer-core")
  testImplementation(platform("io.quarkus:quarkus-bom"))
  testImplementation("io.opentracing:opentracing-api")
  testImplementation("io.opentracing:opentracing-util")
  testImplementation("io.micrometer:micrometer-core")
}

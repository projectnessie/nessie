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
}

extra["maven.name"] = "Nessie - REST Services"

dependencies {
  implementation(platform(rootProject))
  implementation(project(":nessie-model"))
  implementation(project(":nessie-services"))
  implementation(project(":nessie-versioned-spi"))
  implementation("org.slf4j:slf4j-api")
  implementation("jakarta.enterprise:jakarta.enterprise.cdi-api")
  implementation("jakarta.annotation:jakarta.annotation-api")
  implementation("jakarta.validation:jakarta.validation-api")
  implementation("org.jboss.spec.javax.ws.rs:jboss-jaxrs-api_2.1_spec")
  implementation("javax.servlet:javax.servlet-api")
  implementation("com.google.guava:guava")
  compileOnly("org.eclipse.microprofile.openapi:microprofile-openapi-api")
  compileOnly("jakarta.validation:jakarta.validation-api")

  implementation(platform("com.fasterxml.jackson:jackson-bom"))
  implementation("com.fasterxml.jackson.core:jackson-databind")
  compileOnly("com.fasterxml.jackson.core:jackson-annotations")

  testImplementation(platform(rootProject))
  testCompileOnly("org.eclipse.microprofile.openapi:microprofile-openapi-api")

  testImplementation(platform("org.junit:junit-bom"))
  testImplementation("org.assertj:assertj-core")
  testImplementation("org.junit.jupiter:junit-jupiter-api")
  testImplementation("org.junit.jupiter:junit-jupiter-params")
  testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine")
}

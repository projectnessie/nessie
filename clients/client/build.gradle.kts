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
  signing
  `nessie-conventions`
}

extra["maven.name"] = "Nessie - Client"

dependencies {
  api(project(":nessie-model"))

  if (project.hasProperty("jackson-tests")) {
    val jacksonVersion = project.property("jackson-tests") as String
    if (jacksonVersion.isEmpty()) {
      throw GradleException(
        "Project property 'jackson-tests' must contain the Jackson version, must not be empty"
      )
    }
    // No jackson-bom here
    implementation("com.fasterxml.jackson.core:jackson-core:$jacksonVersion!!")
    implementation("com.fasterxml.jackson.core:jackson-annotations:$jacksonVersion!!")
    implementation("com.fasterxml.jackson.core:jackson-databind:$jacksonVersion!!")
  } else {
    implementation(platform(libs.jackson.bom))
    implementation(libs.jackson.core)
    implementation(libs.jackson.databind)
    implementation(libs.jackson.annotations)
  }
  implementation(libs.microprofile.openapi)
  compileOnly(libs.jakarta.validation.api)
  implementation(libs.javax.ws.rs)
  implementation(libs.findbugs.jsr305)
  compileOnly(libs.errorprone.annotations)

  compileOnly(libs.immutables.builder)
  compileOnly(libs.immutables.value.annotations)
  annotationProcessor(libs.immutables.value.processor)

  testImplementation(libs.guava)
  testImplementation(libs.bouncycastle.bcprov)
  testImplementation(libs.bouncycastle.bcpkix)
  testImplementation(libs.mockito.core)

  testImplementation(platform(libs.junit.bom))
  testImplementation(libs.bundles.junit.testing)
  testRuntimeOnly(libs.junit.jupiter.engine)

  compileOnly(libs.quarkus.smallrye.opentracing)
  compileOnly(platform(libs.awssdk.bom))
  compileOnly(libs.awssdk.auth)
  testImplementation(libs.quarkus.smallrye.opentracing)
  testImplementation(platform(libs.awssdk.bom))
  testImplementation(libs.awssdk.auth)
}

jandex { skipDefaultProcessing() }

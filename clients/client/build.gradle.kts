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
  id("org.projectnessie.nessie-project")
  id("org.projectnessie.buildsupport.attach-test-jar")
}

extra["maven.artifactId"] = "nessie-client"

extra["maven.name"] = "Nessie - Client"

dependencies {
  compileOnly(platform(rootProject))
  annotationProcessor(platform(rootProject))
  implementation(platform(rootProject))

  api(projects.model)

  if (project.hasProperty("jackson-tests")) {
    val jacksonVersion = project.property("jackson-tests") as String
    if (jacksonVersion.isEmpty()) {
      throw GradleException(
        "Project property 'jackson-tests' must contain the Jackson version, must not be empty"
      )
    }
    // No jackson-bom here
    implementation("com.fasterxml.jackson.core:jackson-core") {
      version { strictly(jacksonVersion) }
    }
    implementation("com.fasterxml.jackson.core:jackson-annotations") {
      version { strictly(jacksonVersion) }
    }
    implementation("com.fasterxml.jackson.core:jackson-databind") {
      version { strictly(jacksonVersion) }
    }
  } else {
    compileOnly(platform("com.fasterxml.jackson:jackson-bom"))
    implementation("com.fasterxml.jackson.core:jackson-core")
    implementation("com.fasterxml.jackson.core:jackson-annotations")
    implementation("com.fasterxml.jackson.core:jackson-databind")
  }
  implementation("org.eclipse.microprofile.openapi:microprofile-openapi-api")
  compileOnly("jakarta.validation:jakarta.validation-api")
  implementation("javax.ws.rs:javax.ws.rs-api")
  implementation("com.google.code.findbugs:jsr305")
  compileOnly("com.google.errorprone:error_prone_annotations")

  compileOnly("org.immutables:builder")
  compileOnly("org.immutables:value-annotations")
  annotationProcessor("org.immutables:value-processor")

  testImplementation(platform(rootProject))
  testImplementation("com.google.guava:guava")
  testImplementation("org.bouncycastle:bcprov-jdk15on")
  testImplementation("org.bouncycastle:bcpkix-jdk15on")
  testImplementation("org.mockito:mockito-core")

  testImplementation("org.assertj:assertj-core")
  testImplementation(platform("org.junit:junit-bom"))
  testImplementation("org.junit.jupiter:junit-jupiter-api")
  testImplementation("org.junit.jupiter:junit-jupiter-params")
  testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine")

  // Need a few things from Quarkus, but don't leak the dependencies
  compileOnly(platform("io.quarkus:quarkus-bom"))
  compileOnly(platform("software.amazon.awssdk:bom"))
  compileOnly("io.quarkus:quarkus-smallrye-opentracing")
  compileOnly("software.amazon.awssdk:auth")
  testImplementation(platform("io.quarkus:quarkus-bom"))
  testImplementation(platform("software.amazon.awssdk:bom"))
  testImplementation("io.quarkus:quarkus-smallrye-opentracing")
  testImplementation("software.amazon.awssdk:auth")
}

jandex { skipDefaultProcessing() }

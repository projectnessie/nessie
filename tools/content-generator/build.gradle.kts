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

import com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar

plugins {
  `java-library`
  jacoco
  `maven-publish`
  id("com.github.johnrengelman.shadow")
  id("org.projectnessie")
  `nessie-conventions`
}

extra["maven.artifactId"] = "nessie-content-generator"

dependencies {
  implementation(platform(rootProject))
  annotationProcessor(platform(rootProject))
  implementation(projects.clients.client)

  implementation("jakarta.validation:jakarta.validation-api")
  implementation("info.picocli:picocli")
  // TODO help picocli to make their annotation-processor incremental
  annotationProcessor("info.picocli:picocli-codegen")
  implementation("com.google.guava:guava")
  compileOnly("com.google.code.findbugs:jsr305")
  compileOnly("org.eclipse.microprofile.openapi:microprofile-openapi-api")

  implementation(platform("com.fasterxml.jackson:jackson-bom"))
  implementation("com.fasterxml.jackson.core:jackson-annotations")
  implementation("com.fasterxml.jackson.core:jackson-databind")

  testCompileOnly("org.eclipse.microprofile.openapi:microprofile-openapi-api")

  testImplementation("org.assertj:assertj-core")
  testImplementation(platform("org.junit:junit-bom"))
  testImplementation("org.junit.jupiter:junit-jupiter-api")
  testImplementation("org.junit.jupiter:junit-jupiter-params")
  testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine")

  nessieQuarkusServer(
    project(projects.servers.quarkusServer.dependencyProject.path, "quarkusRunner")
  )
}

nessieQuarkusApp {
  includeTask(tasks.named<Test>("intTest"))
  environmentNonInput.put("HTTP_ACCESS_LOG_LEVEL", testLogLevel())
}

tasks.named<ShadowJar>("shadowJar") {
  manifest {
    attributes["Main-Class"] = "org.projectnessie.tools.contentgenerator.cli.NessieContentGenerator"
  }
}

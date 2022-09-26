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
  signing
  alias(libs.plugins.nessie.run)
  `nessie-conventions`
}

applyShadowJar()

dependencies {
  implementation(project(":nessie-client"))

  implementation(libs.jakarta.validation.api)
  implementation(libs.picocli)
  // TODO help picocli to make their annotation-processor incremental
  annotationProcessor(libs.picocli.codegen)
  implementation(libs.guava)
  compileOnly(libs.findbugs.jsr305)
  compileOnly(libs.microprofile.openapi)

  implementation(platform(libs.jackson.bom))
  implementation(libs.jackson.annotations)
  implementation(libs.jackson.databind)

  testCompileOnly(libs.microprofile.openapi)

  testImplementation(platform(libs.junit.bom))
  testImplementation(libs.bundles.junit.testing)
  testRuntimeOnly(libs.junit.jupiter.engine)

  nessieQuarkusServer(project(":nessie-quarkus", "quarkusRunner"))
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

tasks.named<Test>("intTest") { systemProperty("expectedNessieVersion", project.version) }

tasks.named<ProcessResources>("processResources") {
  inputs.property("nessieVersion", project.version)
  expand("nessieVersion" to project.version)
}

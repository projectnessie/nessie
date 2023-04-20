/*
 * Copyright (C) 2023 Dremio
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

import io.smallrye.openapi.api.OpenApiConfig.OperationIdStrategy
import io.smallrye.openapi.gradleplugin.SmallryeOpenApiExtension
import io.smallrye.openapi.gradleplugin.SmallryeOpenApiTask

plugins {
  `java-library`
  jacoco
  `maven-publish`
  signing
  `nessie-conventions`
  alias(libs.plugins.smallrye.openapi)
}

extra["maven.name"] = "Nessie - Events - API"

dependencies {
  implementation(project(":nessie-model"))

  // Immutables
  implementation(libs.immutables.builder)
  implementation(libs.immutables.value.annotations)
  annotationProcessor(libs.immutables.value.processor)

  // Open API
  implementation(libs.microprofile.openapi)

  // Jackson
  implementation(platform(libs.jackson.bom))
  implementation(libs.jackson.annotations)
  implementation(libs.jackson.databind)

  // ErrorProne
  implementation(libs.errorprone.annotations)

  // Testing
  testImplementation(platform(libs.junit.bom))
  testImplementation(libs.bundles.junit.testing)
  testImplementation(libs.junit.jupiter.engine)
  testImplementation(libs.jackson.datatype.jdk8)
  testImplementation(libs.jackson.datatype.jsr310)
}

extensions.configure<SmallryeOpenApiExtension> {
  scanDependenciesDisable.set(false)
  infoVersion.set(project.version.toString())
  schemaFilename.set("META-INF/openapi/openapi")
  operationIdStrategy.set(OperationIdStrategy.METHOD)
  scanPackages.set(listOf("org.projectnessie.events.spi"))
  scanClasses.set(
    listOf(
      "org.projectnessie.model.Content",
      "org.projectnessie.model.ContentKey",
      "org.projectnessie.model.DeltaLakeTable",
      "org.projectnessie.model.IcebergTable",
      "org.projectnessie.model.IcebergView",
      "org.projectnessie.model.Namespace",
    )
  )
}

val openapiSource: Configuration by
  configurations.creating {
    isCanBeConsumed = true
    isCanBeResolved = false
    description = "Source OpenAPI spec files, containing the examples"
  }

val generateOpenApiSpec =
  tasks.named<SmallryeOpenApiTask>("generateOpenApiSpec") {
    inputs.files("src/main").withPathSensitivity(PathSensitivity.RELATIVE)
  }

artifacts { add(openapiSource.name, file("src/main/resources/META-INF")) }

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

import org.projectnessie.buildtools.smallryeopenapi.SmallryeOpenApiTask

plugins {
  `java-library`
  jacoco
  `maven-publish`
  signing
  alias(libs.plugins.nessie.build.smallrye.open.api)
  `nessie-conventions`
}

dependencies {
  implementation(platform(libs.jackson.bom))
  implementation(libs.jackson.databind)
  implementation(libs.jackson.annotations)

  implementation(libs.javax.ws.rs)
  compileOnly(libs.microprofile.openapi)
  compileOnly(libs.jakarta.validation.api)
  implementation(libs.findbugs.jsr305)

  compileOnly(libs.immutables.builder)
  compileOnly(libs.immutables.value.annotations)
  annotationProcessor(libs.immutables.value.processor)

  testCompileOnly(libs.microprofile.openapi)

  testImplementation(platform(libs.junit.bom))
  testImplementation(libs.bundles.junit.testing)
  testRuntimeOnly(libs.junit.jupiter.engine)
}

smallryeOpenApi {
  infoTitle.set("Nessie API")
  infoVersion.set(project.version.toString())
  infoContactName.set("Project Nessie")
  infoContactUrl.set("https://projectnessie.org")
  infoLicenseName.set("Apache 2.0")
  infoLicenseUrl.set("http://www.apache.org/licenses/LICENSE-2.0.html")
  schemaFilename.set("META-INF/openapi/openapi")
  operationIdStrategy.set("METHOD")
  scanPackages.set(
    listOf("org.projectnessie.api", "org.projectnessie.api.http", "org.projectnessie.model")
  )
}

val openapi by
  configurations.creating {
    isCanBeConsumed = true
    isCanBeResolved = false
    description = "Generated OpenAPI spec files"
  }

val openapiSource by
  configurations.creating {
    isCanBeConsumed = true
    isCanBeResolved = false
    description = "Source OpenAPI spec files, containing the examples"
  }

val generateOpenApiSpec =
  tasks.named<SmallryeOpenApiTask>("generateOpenApiSpec") {
    inputs.files("src/main").withPathSensitivity(PathSensitivity.RELATIVE)
  }

artifacts {
  add(openapi.name, generateOpenApiSpec.get().outputDirectory) { builtBy(generateOpenApiSpec) }
  add(openapiSource.name, file("src/main/resources/META-INF"))
}

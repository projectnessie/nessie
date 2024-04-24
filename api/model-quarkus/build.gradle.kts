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

import io.smallrye.openapi.api.OpenApiConfig.OperationIdStrategy
import io.smallrye.openapi.gradleplugin.SmallryeOpenApiExtension
import io.smallrye.openapi.gradleplugin.SmallryeOpenApiTask
import org.apache.tools.ant.filters.ReplaceTokens

plugins {
  id("nessie-conventions-quarkus")
  id("nessie-jacoco")
  alias(libs.plugins.smallrye.openapi)
}

extra["maven.name"] = "Nessie - Model - Variant only for Java 17+ consumers"

description =
  "nessie-model-jakarta is effectively the same as nessie-model, but it is _not_ a " +
    "multi-release jar and retains the jakarta annotations in the canonical classes. " +
    "Please note that this artifact will go away, once Nessie no longer support Java 8 for clients. " +
    "Therefore, do _not_ refer to this artifact - it is only meant for consumption by Nessie-Quarkus."

dependencies {
  implementation(platform(libs.jackson.bom))
  implementation("com.fasterxml.jackson.core:jackson-databind")
  implementation("com.fasterxml.jackson.core:jackson-annotations")

  // javax/jakarta
  compileOnly(libs.jakarta.ws.rs.api)
  compileOnly(libs.javax.ws.rs)
  compileOnly(libs.jakarta.validation.api)
  compileOnly(libs.javax.validation.api)
  compileOnly(libs.jakarta.annotation.api)
  compileOnly(libs.findbugs.jsr305)

  compileOnly(libs.microprofile.openapi)

  compileOnly(libs.immutables.builder)
  compileOnly(libs.immutables.value.annotations)
  annotationProcessor(libs.immutables.value.processor)
}

extensions.configure<SmallryeOpenApiExtension> {
  scanDependenciesDisable = false
  infoVersion = project.version.toString()
  infoDescription =
    "Transactional Catalog for Data Lakes\n" +
      "\n" +
      "* Git-inspired data version control\n" +
      "* Cross-table transactions and visibility\n" +
      "* Works with Apache Iceberg tables"
  schemaFilename = "META-INF/openapi/openapi"
  operationIdStrategy = OperationIdStrategy.METHOD
  scanPackages =
    listOf("org.projectnessie.api", "org.projectnessie.api.http", "org.projectnessie.model")
}

tasks.named<ProcessResources>("processResources").configure {
  inputs.property("projectVersion", project.version)
  filter(ReplaceTokens::class, mapOf("tokens" to mapOf("projectVersion" to project.version)))
}

val openapiSource by
  configurations.creating {
    isCanBeConsumed = true
    isCanBeResolved = false
    description = "Source OpenAPI spec files, containing the examples"
  }

val generateOpenApiSpec = tasks.named<SmallryeOpenApiTask>("generateOpenApiSpec")

generateOpenApiSpec.configure {
  inputs.files("src/main").withPathSensitivity(PathSensitivity.RELATIVE)
}

artifacts { add(openapiSource.name, file("src/main/resources/META-INF")) }

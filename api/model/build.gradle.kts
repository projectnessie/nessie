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
  id("nessie-conventions-client")
  id("nessie-jacoco")
  alias(libs.plugins.annotations.stripper)
  alias(libs.plugins.smallrye.openapi)
}

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

  testCompileOnly(libs.microprofile.openapi)
  testCompileOnly(libs.immutables.value.annotations)
  testAnnotationProcessor(libs.immutables.value.processor)
  testCompileOnly(libs.jakarta.ws.rs.api)
  testCompileOnly(libs.javax.ws.rs)
  testCompileOnly(libs.jakarta.validation.api)
  testCompileOnly(libs.javax.validation.api)
  testCompileOnly(libs.jakarta.annotation.api)
  testCompileOnly(libs.findbugs.jsr305)

  testImplementation(platform(libs.junit.bom))
  testImplementation(libs.bundles.junit.testing)

  intTestImplementation(platform(libs.testcontainers.bom))
  intTestImplementation("org.testcontainers:testcontainers")
  intTestImplementation(libs.awaitility)
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

annotationStripper {
  registerDefault().configure {
    annotationsToDrop("^jakarta[.].+".toRegex())
    unmodifiedClassesForJavaVersion = 11
  }
}

tasks.named<Test>("intTest").configure {
  dependsOn(generateOpenApiSpec)
  systemProperty(
    "openapiSchemaDir",
    project.layout.buildDirectory.dir("generated/openapi/META-INF/openapi").get().toString()
  )
  systemProperty("redoclyConfDir", "$projectDir/src/redocly")
}

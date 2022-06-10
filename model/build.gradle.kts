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
  id("org.projectnessie.smallrye-open-api")
  id("org.projectnessie.nessie-project")
  id("org.projectnessie.buildsupport.attach-test-jar")
}

dependencies {
  compileOnly(platform(rootProject))
  annotationProcessor(platform(rootProject))
  implementation(platform(rootProject))

  implementation(platform("com.fasterxml.jackson:jackson-bom"))
  implementation("com.fasterxml.jackson.core:jackson-databind")
  implementation("com.fasterxml.jackson.core:jackson-annotations")

  implementation("javax.ws.rs:javax.ws.rs-api")
  compileOnly("org.eclipse.microprofile.openapi:microprofile-openapi-api")
  compileOnly("jakarta.validation:jakarta.validation-api")
  implementation("com.google.code.findbugs:jsr305")

  compileOnly("org.immutables:builder")
  compileOnly("org.immutables:value-annotations")
  annotationProcessor("org.immutables:value-processor")

  testImplementation(platform(rootProject))
  testCompileOnly("org.eclipse.microprofile.openapi:microprofile-openapi-api")

  testImplementation("org.assertj:assertj-core")
  testImplementation(platform("org.junit:junit-bom"))
  testImplementation("org.junit.jupiter:junit-jupiter-api")
  testImplementation("org.junit.jupiter:junit-jupiter-params")
  testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine")
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
  scanPackages.set(listOf("org.projectnessie.api", "org.projectnessie.model"))
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
    inputs
      .files("src/main/resources/META-INF/openapi.yaml")
      .withPathSensitivity(PathSensitivity.RELATIVE)
  }

artifacts {
  add(openapi.name, generateOpenApiSpec.get().outputDirectory) { builtBy(generateOpenApiSpec) }
  add(openapiSource.name, file("src/main/resources/META-INF"))
}

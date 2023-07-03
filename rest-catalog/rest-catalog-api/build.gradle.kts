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
  id("nessie-conventions-client")
  id("nessie-jacoco")
  alias(libs.plugins.annotations.stripper)
}

description = "Nessie - REST Catalog - API"

extra["maven.name"] = "Nessie - REST Catalog - API"

val versionIceberg = libs.versions.iceberg.get()

dependencies {
  implementation(nessieProject(":nessie-client"))

  implementation("org.apache.iceberg:iceberg-core:$versionIceberg")

  implementation(libs.guava)

  compileOnly(libs.immutables.builder)
  compileOnly(libs.immutables.value.annotations)
  annotationProcessor(libs.immutables.value.processor)

  implementation(platform(libs.jackson.bom))
  implementation("com.fasterxml.jackson.core:jackson-databind")
  implementation("com.fasterxml.jackson.core:jackson-annotations")

  // javax/jakarta
  compileOnly(libs.jakarta.ws.rs.api)
  compileOnly(libs.javax.ws.rs)
  compileOnly(libs.jakarta.enterprise.cdi.api)
  compileOnly(libs.javax.enterprise.cdi.api)
  compileOnly(libs.jakarta.validation.api)
  compileOnly(libs.javax.validation.api)
  compileOnly(libs.findbugs.jsr305)

  compileOnly(libs.microprofile.openapi)
}

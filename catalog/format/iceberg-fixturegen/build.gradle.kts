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

plugins { id("nessie-conventions-java11") }

description = "Nessie - Catalog - Iceberg table format fixtures"

val versionIceberg = libs.versions.iceberg.get()

dependencies {
  compileOnly(project(":nessie-immutables"))
  annotationProcessor(project(":nessie-immutables", configuration = "processor"))

  implementation(project(":nessie-catalog-format-iceberg"))
  implementation(project(":nessie-catalog-model"))
  implementation(project(":nessie-model"))

  implementation(libs.guava)
  implementation(libs.avro)

  implementation(platform(libs.jackson.bom))
  implementation("com.fasterxml.jackson.core:jackson-annotations")
  implementation("com.fasterxml.jackson.datatype:jackson-datatype-jsr310")

  implementation(libs.jakarta.annotation.api) // 'implementation' for smallrye-config
  compileOnly(libs.jakarta.validation.api)

  compileOnly(libs.errorprone.annotations)
  compileOnly(libs.microprofile.openapi)

  testFixturesApi(platform(libs.junit.bom))
  testFixturesApi(libs.bundles.junit.testing)
}

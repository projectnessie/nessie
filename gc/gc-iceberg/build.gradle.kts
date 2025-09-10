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

publishingHelper { mavenName = "Nessie - GC - Iceberg content functionality" }

dependencies {
  implementation(platform(libs.iceberg.bom))
  implementation("org.apache.iceberg:iceberg-core")

  compileOnly(libs.errorprone.annotations)
  compileOnly(nessieProject("nessie-immutables-std"))
  annotationProcessor(nessieProject("nessie-immutables-std", configuration = "processor"))

  implementation(nessieProject("nessie-model"))
  implementation(nessieProject("nessie-gc-base"))

  implementation(libs.slf4j.api)
  implementation(libs.guava)

  compileOnly(platform(libs.jackson.bom))
  compileOnly("com.fasterxml.jackson.core:jackson-annotations")
  compileOnly(libs.microprofile.openapi)

  compileOnly(libs.jakarta.validation.api)
  compileOnly(libs.jakarta.annotation.api)

  testImplementation(nessieProject("nessie-gc-iceberg-mock"))
  testRuntimeOnly(libs.logback.classic)

  // hadoop-common brings Jackson in ancient versions, pulling in the Jackson BOM to avoid that
  testImplementation(platform(libs.jackson.bom))
  testCompileOnly("com.fasterxml.jackson.core:jackson-annotations")
  testCompileOnly(libs.microprofile.openapi)

  testImplementation(platform(libs.junit.bom))
  testImplementation(libs.bundles.junit.testing)
}

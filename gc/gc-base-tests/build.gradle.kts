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
  `java-library`
  `maven-publish`
  signing
  `nessie-conventions`
}

extra["maven.name"] = "Nessie - GC - Base Implementation Tests"

dependencies {
  implementation(platform(nessieRootProject()))
  compileOnly(nessieProjectPlatform("nessie-deps-build-only", gradle))
  annotationProcessor(nessieProjectPlatform("nessie-deps-build-only", gradle))
  implementation(nessieProjectPlatform("nessie-deps-testing", gradle))
  implementation(platform("org.junit:junit-bom"))
  compileOnly(platform("com.fasterxml.jackson:jackson-bom"))

  implementation(nessieProject("nessie-model"))
  implementation(nessieProject("nessie-gc-base"))

  compileOnly("org.eclipse.microprofile.openapi:microprofile-openapi-api")
  compileOnly("jakarta.validation:jakarta.validation-api")
  compileOnly("com.fasterxml.jackson.core:jackson-annotations")
  compileOnly("com.google.code.findbugs:jsr305")

  implementation("org.assertj:assertj-core")
  implementation("org.junit.jupiter:junit-jupiter-api")
  implementation("org.junit.jupiter:junit-jupiter-params")
}

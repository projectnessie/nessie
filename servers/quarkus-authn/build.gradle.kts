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

plugins { id("nessie-conventions-java21") }

publishingHelper { mavenName = "Nessie - Auth for Quarkus based servers" }

dependencies {
  implementation(project(":nessie-model"))
  implementation(project(":nessie-quarkus-common"))
  implementation(project(":nessie-quarkus-config"))
  implementation(project(":nessie-services"))
  implementation(project(":nessie-versioned-spi"))

  implementation(libs.guava)

  implementation(quarkusPlatform(project))
  implementation("io.quarkus:quarkus-security")
  implementation("io.quarkus:quarkus-vertx-http")

  implementation(platform(libs.cel.bom))
  implementation("org.projectnessie.cel:cel-standalone")

  compileOnly(libs.microprofile.openapi)

  implementation(platform(libs.jackson.bom))
  implementation("com.fasterxml.jackson.core:jackson-annotations")

  testFixturesApi(platform(libs.junit.bom))
  testFixturesApi(libs.bundles.junit.testing)
}

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

publishingHelper { mavenName = "Nessie - Quarkus Config Types" }

dependencies {
  implementation(project(":nessie-services-config"))
  implementation(project(":nessie-versioned-storage-common"))

  compileOnly(project(":nessie-doc-generator-annotations"))

  compileOnly(quarkusPlatform(project))
  compileOnly("io.quarkus:quarkus-core")
  compileOnly("io.smallrye.config:smallrye-config-core")

  compileOnly(nessieProject("nessie-immutables-std"))
  annotationProcessor(nessieProject("nessie-immutables-std", configuration = "processor"))

  implementation(libs.guava)

  testFixturesApi(platform(libs.junit.bom))
  testFixturesApi(libs.bundles.junit.testing)

  testFixturesApi(quarkusPlatform(project))
  testFixturesApi("io.quarkus:quarkus-core")

  testRuntimeOnly(libs.logback.classic)
}

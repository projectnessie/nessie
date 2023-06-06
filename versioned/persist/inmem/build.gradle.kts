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
  id("nessie-conventions-server8")
  id("nessie-jacoco")
}

extra["maven.name"] = "Nessie - Versioned - Persist - In-Memory"

dependencies {
  implementation(project(":nessie-versioned-persist-adapter"))
  implementation(project(":nessie-versioned-persist-non-transactional"))
  implementation(project(":nessie-versioned-persist-serialize"))
  implementation(project(":nessie-versioned-spi"))
  implementation(project(":nessie-model"))
  implementation(libs.guava)
  compileOnly(libs.immutables.value.annotations)
  annotationProcessor(libs.immutables.value.processor)

  // javax/jakarta
  compileOnly(libs.jakarta.annotation.api)
  compileOnly(libs.findbugs.jsr305)

  testImplementation(project(":nessie-versioned-tests"))
  testImplementation(project(":nessie-versioned-persist-testextension"))
  testImplementation(project(":nessie-versioned-persist-tests"))
  testImplementation(project(":nessie-versioned-persist-non-transactional-test"))
  testImplementation(project(":nessie-versioned-persist-in-memory-test"))

  testImplementation(platform(libs.junit.bom))
  testImplementation(libs.bundles.junit.testing)
}

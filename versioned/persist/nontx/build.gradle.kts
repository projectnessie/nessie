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

extra["maven.name"] = "Nessie - Versioned - Persist - Non-Transactional"

dependencies {
  implementation(project(":nessie-model"))
  implementation(project(":nessie-versioned-persist-adapter"))
  implementation(project(":nessie-versioned-persist-serialize"))
  implementation(project(":nessie-versioned-spi"))
  implementation(libs.guava)
  compileOnly(libs.immutables.value.annotations)
  annotationProcessor(libs.immutables.value.processor)

  testImplementation(project(":nessie-versioned-persist-testextension"))
  testImplementation(project(":nessie-versioned-persist-tests"))

  testImplementation(platform(libs.junit.bom))
  testImplementation(libs.bundles.junit.testing)
}

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

plugins { id("nessie-conventions-server") }

extra["maven.name"] = "Nessie - Versioned - Persist - Tests"

dependencies {
  implementation(project(":nessie-model"))
  implementation(project(":nessie-versioned-persist-adapter"))
  implementation(project(":nessie-versioned-persist-store"))
  implementation(project(":nessie-versioned-persist-testextension"))
  implementation(project(":nessie-versioned-spi"))
  implementation(project(":nessie-versioned-tests"))
  implementation(project(":nessie-server-store"))
  implementation(libs.guava)
  implementation(libs.micrometer.core)
  implementation(libs.opentracing.mock)

  compileOnly(platform(libs.jackson.bom))
  compileOnly("com.fasterxml.jackson.core:jackson-annotations")

  compileOnly(libs.microprofile.openapi)

  implementation(platform(libs.junit.bom))
  implementation(libs.bundles.junit.testing)
}

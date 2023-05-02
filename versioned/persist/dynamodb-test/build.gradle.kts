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
  id("nessie-conventions-server")
  id("nessie-jacoco")
}

extra["maven.name"] = "Nessie - Versioned - Persist - DynamoDB/test-support"

dependencies {
  implementation(project(":nessie-versioned-persist-adapter"))
  implementation(project(":nessie-versioned-persist-non-transactional"))
  implementation(project(":nessie-versioned-persist-dynamodb"))
  implementation(project(":nessie-versioned-persist-testextension"))
  implementation(project(":nessie-versioned-persist-non-transactional-test"))

  implementation(platform(libs.awssdk.bom))
  implementation("software.amazon.awssdk:dynamodb") {
    exclude("software.amazon.awssdk", "apache-client")
  }

  implementation(libs.testcontainers.testcontainers)
  implementation(libs.docker.java.api)
}

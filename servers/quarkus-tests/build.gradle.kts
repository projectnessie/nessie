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

plugins { id("nessie-conventions-quarkus") }

extra["maven.name"] = "Nessie - Quarkus Tests"

dependencies {
  compileOnly(project(":nessie-quarkus-common"))
  implementation(project(":nessie-versioned-tests"))
  compileOnly(project(":nessie-versioned-persist-adapter"))
  compileOnly(project(":nessie-versioned-persist-testextension"))
  compileOnly(project(":nessie-versioned-persist-dynamodb"))
  compileOnly(project(":nessie-versioned-persist-dynamodb-test"))
  compileOnly(project(":nessie-versioned-persist-mongodb"))
  compileOnly(project(":nessie-versioned-persist-mongodb-test"))
  compileOnly(project(":nessie-versioned-persist-transactional"))
  compileOnly(project(":nessie-versioned-persist-transactional-test"))
  compileOnly(project(":nessie-versioned-storage-bigtable"))
  compileOnly(project(":nessie-versioned-storage-cassandra"))
  compileOnly(project(":nessie-versioned-storage-dynamodb"))
  compileOnly(project(":nessie-versioned-storage-mongodb"))
  compileOnly(project(":nessie-versioned-storage-testextension"))

  compileOnly(enforcedPlatform(libs.quarkus.bom))
  compileOnly("io.quarkus:quarkus-junit5")

  implementation(platform(libs.testcontainers.bom))
  implementation("org.testcontainers:testcontainers")
  compileOnly("org.testcontainers:cassandra")
  compileOnly("org.testcontainers:postgresql")
  compileOnly("org.testcontainers:mongodb")
  implementation(libs.docker.java.api)
  compileOnly(project(":nessie-keycloak-testcontainer"))
  compileOnly(project(":nessie-nessie-testcontainer"))

  compileOnly(platform(libs.awssdk.bom))
  compileOnly("software.amazon.awssdk:dynamodb") {
    exclude("software.amazon.awssdk", "apache-client")
  }

  compileOnly(platform(libs.quarkus.amazon.services.bom))
  compileOnly("io.quarkiverse.amazonservices:quarkus-amazon-dynamodb")

  implementation(libs.testcontainers.keycloak)
  implementation(libs.keycloak.admin.client)
  // Keycloak-admin-client depends on Resteasy.
  // Need to bump Resteasy, because Resteasy < 6.2.4 clashes with our Jackson version management and
  // cause non-existing jackson versions like 2.15.2-jakarta, which then lets the build fail.
  implementation(platform(libs.resteasy.bom))
}

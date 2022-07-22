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
  jacoco
  `maven-publish`
  signing
  `nessie-conventions`
}

extra["maven.name"] = "Nessie - Versioned - Persist - Tests"

dependencies {
  implementation(platform(rootProject))

  implementation(project(":nessie-versioned-persist-adapter"))
  implementation(project(":nessie-versioned-persist-store"))
  implementation(project(":nessie-versioned-spi"))
  implementation(project(":nessie-versioned-tests"))
  implementation("com.google.guava:guava")
  implementation(platform("io.quarkus:quarkus-bom"))
  implementation("io.micrometer:micrometer-core")
  implementation("io.opentracing:opentracing-mock")
  implementation("com.google.protobuf:protobuf-java")

  implementation("org.assertj:assertj-core")
  implementation("org.mockito:mockito-core")
  implementation(platform("org.junit:junit-bom"))
  implementation("org.junit.jupiter:junit-jupiter-api")
  implementation("org.junit.jupiter:junit-jupiter-params")
}

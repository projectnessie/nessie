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

extra["maven.name"] = "Nessie - Versioned - Persist - Transactional/test-support"

dependencies {
  implementation(platform(rootProject))
  implementation(platform(project(":nessie-deps-persist")))
  compileOnly(platform(project(":nessie-deps-build-only")))
  annotationProcessor(platform(project(":nessie-deps-build-only")))
  implementation(platform(project(":nessie-deps-testing")))

  implementation(project(":nessie-versioned-persist-adapter"))
  implementation(project(":nessie-versioned-persist-transactional"))
  implementation(project(":nessie-versioned-persist-testextension"))
  implementation(project(":nessie-versioned-spi"))
  implementation(project(":nessie-versioned-tests"))

  implementation("com.google.guava:guava")
  implementation("com.google.code.findbugs:jsr305")
  compileOnly("org.immutables:value-annotations")
  annotationProcessor("org.immutables:value-processor")
  implementation("org.slf4j:slf4j-api")

  implementation("io.agroal:agroal-pool")
  compileOnly("com.h2database:h2")
  compileOnly("org.postgresql:postgresql")

  implementation("org.testcontainers:postgresql")
  implementation("org.testcontainers:cockroachdb")
  implementation("com.github.docker-java:docker-java-api")
}

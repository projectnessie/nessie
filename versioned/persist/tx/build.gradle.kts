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

extra["maven.name"] = "Nessie - Versioned - Persist - Transactional"

dependencies {
  implementation(platform(rootProject))
  implementation(platform(project(":nessie-deps-persist")))
  compileOnly(platform(project(":nessie-deps-build-only")))
  annotationProcessor(platform(project(":nessie-deps-build-only")))

  implementation(project(":nessie-versioned-persist-adapter"))
  implementation(project(":nessie-versioned-persist-serialize"))
  implementation(project(":nessie-versioned-spi"))
  implementation("com.google.guava:guava")
  implementation("com.google.code.findbugs:jsr305")
  compileOnly("org.immutables:value-annotations")
  annotationProcessor("org.immutables:value-processor")
  implementation("org.slf4j:slf4j-api")

  compileOnly("io.agroal:agroal-pool")
  compileOnly("com.h2database:h2")
  compileOnly("org.postgresql:postgresql")

  testCompileOnly(platform(project(":nessie-deps-build-only")))
  testAnnotationProcessor(platform(project(":nessie-deps-build-only")))
  testImplementation(platform(project(":nessie-deps-testing")))
  testImplementation(platform("org.junit:junit-bom"))

  testImplementation(project(":nessie-versioned-tests"))
  testCompileOnly("org.immutables:value-annotations")
  testAnnotationProcessor("org.immutables:value-processor")
  testImplementation(project(":nessie-versioned-persist-testextension"))
  testImplementation(project(":nessie-versioned-persist-tests"))
  testImplementation(project(":nessie-versioned-persist-transactional-test"))
  testRuntimeOnly("com.h2database:h2")
  testRuntimeOnly("org.postgresql:postgresql")

  testImplementation("org.assertj:assertj-core")
  testImplementation("org.junit.jupiter:junit-jupiter-api")
  testImplementation("org.junit.jupiter:junit-jupiter-params")
  testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine")
}

tasks.named<Test>("test") { maxParallelForks = Runtime.getRuntime().availableProcessors() }

tasks.named<Test>("intTest") {
  systemProperty("it.nessie.dbs", System.getProperty("it.nessie.dbs", "postgres"))
  systemProperty(
    "it.nessie.container.postgres.tag",
    System.getProperty(
      "it.nessie.container.postgres.tag",
      dependencyVersion("versionPostgresContainerTag")
    )
  )
}

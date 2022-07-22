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
  id("org.projectnessie.buildsupport.attach-test-jar")
}

extra["maven.name"] = "Nessie - Versioned - Persist - Rocks"

dependencies {
  implementation(platform(rootProject))
  annotationProcessor(platform(rootProject))

  implementation(project(":nessie-versioned-persist-adapter"))
  implementation(project(":nessie-versioned-persist-non-transactional"))
  implementation(project(":nessie-versioned-persist-serialize"))
  implementation(project(":nessie-versioned-spi"))
  implementation("com.google.guava:guava")
  compileOnly("org.immutables:value-annotations")
  annotationProcessor("org.immutables:value-processor")
  implementation("com.google.code.findbugs:jsr305")
  implementation("org.rocksdb:rocksdbjni")
  compileOnly("org.graalvm.nativeimage:svm")

  testImplementation(platform(rootProject))
  testImplementation(project(":nessie-versioned-tests"))
  testImplementation(project(":nessie-versioned-persist-tests"))
  testImplementation(project(":nessie-versioned-persist-non-transactional")) { testJarCapability() }

  testImplementation(platform("org.junit:junit-bom"))
  testImplementation("org.assertj:assertj-core")
  testImplementation("org.junit.jupiter:junit-jupiter-api")
  testImplementation("org.junit.jupiter:junit-jupiter-params")
  testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine")
}

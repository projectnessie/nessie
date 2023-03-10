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

extra["maven.name"] = "Nessie - Multi-Environment Test Engine"

dependencies {
  api(libs.slf4j.api)

  implementation(platform(libs.junit.bom))
  api(libs.junit.jupiter.api)
  compileOnly(libs.junit.jupiter.engine)
  implementation(libs.junit.platform.launcher)

  testImplementation(libs.junit.platform.testkit)
  testCompileOnly(libs.junit.jupiter.engine)
}

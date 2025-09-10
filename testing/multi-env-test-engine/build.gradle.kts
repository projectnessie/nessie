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

plugins { id("nessie-conventions-java11") }

publishingHelper { mavenName = "Nessie - Multi-Environment Test Engine" }

dependencies {
  api(libs.slf4j.api)

  api(platform(libs.junit.bom))
  api("org.junit.jupiter:junit-jupiter-api")
  compileOnly("org.junit.jupiter:junit-jupiter-engine")
  implementation("org.junit.platform:junit-platform-launcher")

  testImplementation("org.junit.platform:junit-platform-testkit")
  testCompileOnly("org.junit.jupiter:junit-jupiter-engine")
}

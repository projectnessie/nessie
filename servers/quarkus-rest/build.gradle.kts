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

plugins { id("nessie-conventions-java21") }

publishingHelper { mavenName = "Nessie - Quarkus REST" }

dependencies {
  implementation(project(":nessie-model"))
  implementation(project(":nessie-rest-common"))

  implementation(quarkusPlatform(project))
  implementation("io.quarkus:quarkus-core")
  implementation("io.quarkus:quarkus-rest")
  implementation("io.quarkus:quarkus-rest-jackson")

  implementation("com.fasterxml.jackson.core:jackson-databind")
  compileOnly("com.fasterxml.jackson.core:jackson-annotations")
}

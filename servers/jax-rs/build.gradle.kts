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
  alias(libs.plugins.annotations.stripper)
}

extra["maven.name"] = "Nessie - JAX-RS"

description = "Nessie on Glassfish/Jersey/Weld"

dependencies {
  api(project(":nessie-client"))
  api(project(":nessie-model"))
  api(project(":nessie-rest-services"))
  api(project(":nessie-services"))
  api(project(":nessie-server-store"))
  api(project(":nessie-versioned-spi"))
  api(project(":nessie-versioned-storage-common"))
  api(project(":nessie-versioned-storage-store"))
  api(project(":nessie-versioned-storage-testextension"))
  implementation(libs.slf4j.api)

  // javax/jakarta
  compileOnly(libs.jakarta.ws.rs.api)
  compileOnly(libs.javax.ws.rs21)
  compileOnly(libs.jakarta.enterprise.cdi.api)
  compileOnly(libs.javax.enterprise.cdi.api)
  compileOnly(libs.jakarta.annotation.api)
  compileOnly(libs.findbugs.jsr305)
  compileOnly(libs.jakarta.validation.api)
  compileOnly(libs.javax.validation.api)

  compileOnly(libs.microprofile.openapi)

  compileOnly(libs.hibernate.validator.cdi)

  api(platform(libs.jackson.bom))
  api("com.fasterxml.jackson.core:jackson-databind")
  compileOnly("com.fasterxml.jackson.core:jackson-annotations")
}

annotationStripper {
  registerDefault().configure {
    annotationsToDrop("^jakarta[.].+".toRegex())
    unmodifiedClassesForJavaVersion.set(11)
  }
}

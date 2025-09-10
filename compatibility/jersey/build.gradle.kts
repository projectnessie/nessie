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

publishingHelper { mavenName = "Nessie - Backward Compatibility - Jersey" }

dependencies {
  implementation(project(":nessie-model"))
  implementation(project(":nessie-rest-services"))
  implementation(project(":nessie-services"))
  implementation(project(":nessie-services-config"))
  implementation(project(":nessie-server-store"))
  implementation(project(":nessie-versioned-storage-common"))
  implementation(project(":nessie-versioned-storage-store"))
  implementation(project(":nessie-versioned-spi"))

  compileOnly(libs.jakarta.ws.rs.api)
  compileOnly(libs.jakarta.enterprise.cdi.api)
  compileOnly(libs.jakarta.annotation.api)
  compileOnly(libs.jakarta.validation.api)

  implementation(platform(libs.jackson.bom))
  implementation("com.fasterxml.jackson.core:jackson-databind")
  implementation("com.fasterxml.jackson.core:jackson-annotations")

  implementation(libs.microprofile.openapi)

  implementation(platform(libs.jersey.bom))
  implementation("org.glassfish.jersey.core:jersey-server")
  implementation("org.glassfish.jersey.inject:jersey-hk2")
  implementation("org.glassfish.jersey.media:jersey-media-json-jackson")
  implementation("org.glassfish.jersey.ext:jersey-bean-validation")
  implementation("org.glassfish.jersey.ext.cdi:jersey-cdi1x")
  implementation("org.glassfish.jersey.ext.cdi:jersey-cdi-rs-inject")
  implementation("org.glassfish.jersey.ext.cdi:jersey-weld2-se")
  implementation(
    platform("org.glassfish.jersey.test-framework.providers:jersey-test-framework-provider-bundle")
  )
  implementation("org.glassfish.jersey.test-framework:jersey-test-framework-core")
  implementation("org.glassfish.jersey.test-framework:jersey-test-framework-util")
  implementation(
    "org.glassfish.jersey.test-framework.providers:jersey-test-framework-provider-grizzly2"
  )

  implementation("org.jboss.weld.se:weld-se-core")
}

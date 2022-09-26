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

extra["maven.name"] = "Nessie - JAX-RS"

description = "Nessie on Glassfish/Jersey/Weld"

dependencies {
  api(project(":nessie-client"))
  api(project(":nessie-model"))
  api(project(":nessie-rest-services"))
  api(project(":nessie-services"))
  api(project(":nessie-server-store"))
  api(project(":nessie-versioned-spi"))
  api(project(":nessie-versioned-persist-store"))
  api(project(":nessie-versioned-persist-testextension"))
  api(project(":nessie-versioned-persist-adapter"))
  api(project(":nessie-versioned-persist-serialize"))
  api(project(":nessie-versioned-persist-in-memory"))
  api(project(":nessie-versioned-persist-in-memory-test"))
  api(project(":nessie-versioned-persist-rocks"))
  api(project(":nessie-versioned-persist-rocks-test"))
  api(project(":nessie-versioned-persist-dynamodb"))
  api(project(":nessie-versioned-persist-dynamodb-test"))
  api(project(":nessie-versioned-persist-mongodb"))
  api(project(":nessie-versioned-persist-mongodb-test"))
  api(project(":nessie-versioned-persist-non-transactional"))
  api(project(":nessie-versioned-persist-non-transactional-test"))
  api(project(":nessie-versioned-persist-transactional"))
  api(project(":nessie-versioned-persist-transactional-test"))
  implementation(libs.slf4j.api)
  implementation(libs.javax.ws.rs21)
  api(libs.jakarta.enterprise.cdi.api)
  api(libs.jakarta.annotation.api)
  api(libs.jakarta.validation.api)

  api(platform(libs.jackson.bom))
  api(libs.jackson.databind)
  compileOnly(libs.jackson.annotations)

  api(platform(libs.jersey.bom))
  api("org.glassfish.jersey.core:jersey-server")
  api("org.glassfish.jersey.inject:jersey-hk2")
  api("org.glassfish.jersey.media:jersey-media-json-jackson")
  api("org.glassfish.jersey.ext:jersey-bean-validation")
  api("org.glassfish.jersey.ext.cdi:jersey-cdi1x")
  api("org.glassfish.jersey.ext.cdi:jersey-cdi-rs-inject")
  api("org.glassfish.jersey.ext.cdi:jersey-weld2-se")

  api(
    platform("org.glassfish.jersey.test-framework.providers:jersey-test-framework-provider-bundle")
  )
  api("org.glassfish.jersey.test-framework:jersey-test-framework-core")
  api("org.glassfish.jersey.test-framework:jersey-test-framework-util")
  api("org.glassfish.jersey.test-framework.providers:jersey-test-framework-provider-grizzly2")
  api("org.glassfish.jersey.test-framework.providers:jersey-test-framework-provider-inmemory")
  api("org.glassfish.jersey.test-framework.providers:jersey-test-framework-provider-external")
  api("org.glassfish.jersey.test-framework.providers:jersey-test-framework-provider-jdk-http")
  api("org.glassfish.jersey.test-framework.providers:jersey-test-framework-provider-simple")
  api("org.glassfish.jersey.test-framework.providers:jersey-test-framework-provider-jetty")

  api("org.jboss.weld.se:weld-se-core")

  compileOnly(libs.microprofile.openapi)
  compileOnly(libs.jakarta.validation.api)
}

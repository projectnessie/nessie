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
}

extra["maven.name"] = "Nessie - JUnit Jupiter Test Extension"

description = "JUnit Jupiter Extension to run tests against an \"embedded\" Nessie instance."

dependencies {
  api(project(":nessie-jaxrs"))
  api(project(":nessie-client-testextension"))

  api(platform(libs.junit.bom))
  api("org.junit.jupiter:junit-jupiter-api")

  api(libs.slf4j.api)
  api(libs.assertj.core)

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

  api(libs.hibernate.validator.cdi)

  compileOnly(libs.microprofile.openapi)

  testCompileOnly(libs.microprofile.openapi)
  testImplementation(project(":nessie-versioned-storage-inmemory"))
  testImplementation(project(":nessie-versioned-storage-testextension"))

  testImplementation(platform(libs.junit.bom))
  testImplementation(libs.bundles.junit.testing)
}

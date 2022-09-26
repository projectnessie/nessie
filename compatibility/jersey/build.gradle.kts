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

extra["maven.name"] = "Nessie - Backward Compatibility - Jersey"

dependencies {
  implementation(project(":nessie-model"))
  implementation(project(":nessie-rest-services"))
  implementation(project(":nessie-services"))
  implementation(project(":nessie-server-store"))
  implementation(project(":nessie-versioned-persist-adapter"))
  implementation(project(":nessie-versioned-persist-store"))
  implementation(project(":nessie-versioned-persist-testextension"))
  implementation(project(":nessie-versioned-spi"))
  implementation(libs.javax.ws.rs21)
  implementation(libs.jakarta.enterprise.cdi.api)
  implementation(libs.jakarta.annotation.api)
  implementation(libs.jakarta.validation.api)
  implementation(libs.javax.ws.rs)

  implementation(platform(libs.jackson.bom))
  implementation(libs.jackson.databind)
  implementation(libs.jackson.annotations)

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
  implementation(
    "org.glassfish.jersey.test-framework.providers:jersey-test-framework-provider-inmemory"
  )
  implementation(
    "org.glassfish.jersey.test-framework.providers:jersey-test-framework-provider-external"
  )
  implementation(
    "org.glassfish.jersey.test-framework.providers:jersey-test-framework-provider-jdk-http"
  )
  implementation(
    "org.glassfish.jersey.test-framework.providers:jersey-test-framework-provider-simple"
  )
  implementation(
    "org.glassfish.jersey.test-framework.providers:jersey-test-framework-provider-jetty"
  )

  implementation("org.jboss.weld.se:weld-se-core")
}

tasks.withType<Test>().configureEach {
  systemProperty("rocksdb.version", libs.versions.rocksdb.get())
}

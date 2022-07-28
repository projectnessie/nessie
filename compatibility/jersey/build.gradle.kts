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
  implementation(platform(rootProject))
  implementation(platform(project(":nessie-deps-testing")))
  implementation(platform("org.glassfish.jersey:jersey-bom"))
  implementation(platform("com.fasterxml.jackson:jackson-bom"))
  implementation(
    platform("org.glassfish.jersey.test-framework.providers:jersey-test-framework-provider-bundle")
  )

  implementation(project(":nessie-model"))
  implementation(project(":nessie-rest-services"))
  implementation(project(":nessie-services"))
  implementation(project(":nessie-server-store"))
  implementation(project(":nessie-versioned-persist-adapter"))
  implementation(project(":nessie-versioned-persist-store"))
  implementation(project(":nessie-versioned-persist-tests"))
  implementation(project(":nessie-versioned-spi"))
  implementation("org.jboss.spec.javax.ws.rs:jboss-jaxrs-api_2.1_spec")
  implementation("jakarta.enterprise:jakarta.enterprise.cdi-api")
  implementation("jakarta.annotation:jakarta.annotation-api")
  implementation("jakarta.validation:jakarta.validation-api")
  implementation("javax.ws.rs:javax.ws.rs-api")
  implementation("com.fasterxml.jackson.core:jackson-databind")
  implementation("org.glassfish.jersey.core:jersey-server")
  implementation("org.glassfish.jersey.inject:jersey-hk2")
  implementation("org.glassfish.jersey.media:jersey-media-json-jackson")
  implementation("org.glassfish.jersey.ext:jersey-bean-validation")
  implementation("org.glassfish.jersey.ext.cdi:jersey-cdi1x")
  implementation("org.glassfish.jersey.ext.cdi:jersey-cdi-rs-inject")
  implementation("org.glassfish.jersey.ext.cdi:jersey-weld2-se")
  implementation("org.glassfish.jersey.test-framework:jersey-test-framework-core")
  implementation("org.glassfish.jersey.test-framework:jersey-test-framework-util")
  implementation("com.fasterxml.jackson.core:jackson-annotations")
  implementation("org.eclipse.microprofile.openapi:microprofile-openapi-api")

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
  systemProperty("rocksdb.version", dependencyVersion("versionRocksDb"))
}

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
  id("org.projectnessie.nessie-project")
}

extra["maven.artifactId"] = "nessie-jaxrs"

extra["maven.name"] = "Nessie - JAX-RS"

description = "Nessie on Glassfish/Jersey/Weld"

dependencies {
  api(platform(rootProject))
  implementation(platform(rootProject))
  api(projects.clients.client)
  api(projects.model)
  api(projects.servers.restServices)
  api(projects.servers.services)
  api(projects.servers.store)
  api(projects.versioned.spi)
  api(projects.versioned.persist.persistStore)
  api(projects.versioned.persist.persistTests)
  api(projects.versioned.persist.adapter)
  api(projects.versioned.persist.serialize)
  api(projects.versioned.persist.inmem)
  api(projects.versioned.persist.inmem) { testJarCapability() }
  api(projects.versioned.persist.rocks)
  api(projects.versioned.persist.rocks) { testJarCapability() }
  api(projects.versioned.persist.dynamodb)
  api(projects.versioned.persist.dynamodb) { testJarCapability() }
  api(projects.versioned.persist.mongodb)
  api(projects.versioned.persist.mongodb) { testJarCapability() }
  api(projects.versioned.persist.nontx)
  api(projects.versioned.persist.tx)
  api(projects.versioned.persist.tx) { testJarCapability() }
  implementation("org.slf4j:slf4j-api")
  implementation("org.jboss.spec.javax.ws.rs:jboss-jaxrs-api_2.1_spec")
  api(platform("org.glassfish.jersey:jersey-bom"))
  api("jakarta.enterprise:jakarta.enterprise.cdi-api")
  api("jakarta.annotation:jakarta.annotation-api")
  api("jakarta.validation:jakarta.validation-api")
  api("com.fasterxml.jackson.core:jackson-databind")
  api("org.glassfish.jersey.core:jersey-server")
  api("org.glassfish.jersey.inject:jersey-hk2")
  api("org.glassfish.jersey.media:jersey-media-json-jackson")
  api("org.glassfish.jersey.ext:jersey-bean-validation")
  api("org.glassfish.jersey.ext.cdi:jersey-cdi1x")
  api("org.glassfish.jersey.ext.cdi:jersey-cdi-rs-inject")
  api("org.glassfish.jersey.ext.cdi:jersey-weld2-se")
  api("org.glassfish.jersey.test-framework:jersey-test-framework-core")
  api("org.glassfish.jersey.test-framework:jersey-test-framework-util")

  compileOnly("com.fasterxml.jackson.core:jackson-annotations")
  compileOnly("org.eclipse.microprofile.openapi:microprofile-openapi-api")
  compileOnly("jakarta.validation:jakarta.validation-api")

  api(
    platform("org.glassfish.jersey.test-framework.providers:jersey-test-framework-provider-bundle")
  )
  api("org.glassfish.jersey.test-framework.providers:jersey-test-framework-provider-grizzly2")
  api("org.glassfish.jersey.test-framework.providers:jersey-test-framework-provider-inmemory")
  api("org.glassfish.jersey.test-framework.providers:jersey-test-framework-provider-external")
  api("org.glassfish.jersey.test-framework.providers:jersey-test-framework-provider-jdk-http")
  api("org.glassfish.jersey.test-framework.providers:jersey-test-framework-provider-simple")
  api("org.glassfish.jersey.test-framework.providers:jersey-test-framework-provider-jetty")

  api("org.jboss.weld.se:weld-se-core")
}

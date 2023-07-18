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

import io.quarkus.gradle.tasks.QuarkusBuild
import org.apache.tools.ant.taskdefs.condition.Os

plugins {
  alias(libs.plugins.quarkus)
  id("nessie-conventions-quarkus")
  id("nessie-jacoco")
}

description = "Nessie REST Catalog - Quarkus Server"

extra["maven.name"] = "Nessie - REST Catalog - Quarkus Server"

val icebergVersion = libs.versions.iceberg.get()

val quarkusRunner by
  configurations.creating {
    description = "Used to reference the generated runner-jar (either fast-jar or uber-jar)"
  }

dependencies {
  implementation(nessieProject(":nessie-catalog-service"))
  implementation(nessieProject(":nessie-model"))
  implementation(nessieProject(":nessie-client"))

  implementation("org.apache.iceberg:iceberg-core:$icebergVersion")
  implementation(nessieProject(":nessie-catalog-api"))

  implementation(libs.hadoop.common) {
    exclude("ch.qos.reload4j", "reload4j")
    exclude("com.sun.jersey")
    exclude("commons-cli", "commons-cli")
    exclude("jakarta.activation", "jakarta.activation-api")
    exclude("javax.servlet", "javax.servlet-api")
    exclude("javax.servlet.jsp", "jsp-api")
    exclude("javax.ws.rs", "javax.ws.rs-api")
    exclude("log4j", "log4j")
    exclude("org.slf4j", "slf4j-log4j12")
    exclude("org.slf4j", "slf4j-reload4j")
    exclude("org.eclipse.jetty")
    exclude("org.apache.zookeeper")
  }

  implementation(enforcedPlatform(libs.quarkus.bom))
  implementation(enforcedPlatform(libs.quarkus.amazon.services.bom))
  implementation("io.quarkus:quarkus-resteasy-reactive")
  implementation("io.quarkus:quarkus-resteasy-reactive-jackson")
  implementation("io.quarkus:quarkus-hibernate-validator")
  implementation("io.smallrye:smallrye-open-api-jaxrs")
  implementation("io.quarkus:quarkus-elytron-security-properties-file")
  implementation("io.quarkus:quarkus-smallrye-health")
  implementation("io.quarkus:quarkus-oidc")
  implementation("io.quarkus:quarkus-smallrye-openapi")
  implementation("io.quarkus:quarkus-micrometer")
  implementation("io.quarkus:quarkus-opentelemetry")
  implementation(libs.quarkus.logging.sentry)
  implementation("io.micrometer:micrometer-registry-prometheus")

  if (project.hasProperty("k8s")) {
    /*
     * Use the k8s project property to generate manifest files for K8s & Minikube, which will be
     * located under target/kubernetes.
     * See also https://quarkus.io/guides/deploying-to-kubernetes for additional details
     */
    implementation("io.quarkus:quarkus-kubernetes")
    implementation("io.quarkus:quarkus-minikube")
  }

  implementation(libs.guava)

  compileOnly(libs.microprofile.openapi)

  testFixturesApi("org.apache.iceberg:iceberg-api:$icebergVersion:tests")
  testFixturesApi("org.apache.iceberg:iceberg-core:$icebergVersion:tests")

  testFixturesApi(enforcedPlatform(libs.quarkus.bom))
  testFixturesApi("io.quarkus:quarkus-junit5")
  testFixturesApi("io.quarkus:quarkus-jacoco")

  testFixturesApi(nessieProject(":nessie-quarkus-tests"))

  testFixturesCompileOnly(libs.microprofile.openapi)

  testFixturesImplementation(platform(libs.junit.bom))
  testFixturesImplementation(libs.bundles.junit.testing)

  intTestImplementation(nessieProject(":nessie-keycloak-testcontainer")) {
    exclude(
      group = "org.keycloak",
      module = "keycloak-admin-client"
    ) // Quarkus 3 / Jakarta EE required
  }
  intTestImplementation(libs.keycloak.admin.client.jakarta)
  intTestImplementation(nessieProject(":nessie-nessie-testcontainer"))
  // Keycloak-admin-client depends on Resteasy.
  // Need to bump Resteasy, because Resteasy < 6.2.4 clashes with our Jackson version management and
  // cause non-existing jackson versions like 2.15.2-jakarta, which then lets the build fail.
  intTestImplementation(platform(libs.resteasy.bom))
}

val packageType = quarkusPackageType()

quarkus { quarkusBuildProperties.put("quarkus.package.type", packageType) }

val quarkusBuild =
  tasks.named<QuarkusBuild>("quarkusBuild") {
    outputs.doNotCacheIf("Do not add huge cache artifacts to build cache") { true }
    inputs.property("quarkus.package.type", packageType)
    inputs.property("final.name", quarkus.finalName())
  }

tasks.withType<Test>().configureEach {
  systemProperty("keycloak.docker.tag", libs.versions.keycloak.get())
}

// Expose runnable jar via quarkusRunner configuration for integration-tests that require the
// server.
artifacts {
  add(
    quarkusRunner.name,
    provider {
      if (quarkusFatJar()) quarkusBuild.get().runnerJar
      else quarkusBuild.get().fastJar.resolve("quarkus-run.jar")
    }
  ) {
    builtBy(quarkusBuild)
  }
}

// Add the uber-jar, if built, to the Maven publication
if (quarkusFatJar()) {
  afterEvaluate {
    publishing {
      publications {
        named<MavenPublication>("maven") {
          artifact(quarkusBuild.get().runnerJar) {
            classifier = "runner"
            builtBy(quarkusBuild)
          }
        }
      }
    }
  }
}

listOf("javadoc", "sourcesJar").forEach { name ->
  tasks.named(name) { dependsOn(tasks.named("compileQuarkusGeneratedSourcesJava")) }
}

listOf("checkstyleTest", "compileTestJava").forEach { name ->
  tasks.named(name) { dependsOn(tasks.named("compileQuarkusTestGeneratedSourcesJava")) }
}

// Testcontainers is not supported on Windows :(
if (Os.isFamily(Os.FAMILY_WINDOWS)) {
  tasks.withType<Test>().configureEach { this.enabled = false }
}

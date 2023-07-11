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

extra["maven.name"] = "Nessie - Quarkus Server"

val quarkusRunner by
  configurations.creating {
    description = "Used to reference the generated runner-jar (either fast-jar or uber-jar)"
  }

val openapiSource by
  configurations.creating { description = "Used to reference OpenAPI spec files" }

dependencies {
  implementation(project(":nessie-model"))
  implementation(project(":nessie-services"))
  implementation(project(":nessie-quarkus-common"))
  implementation(project(":nessie-events-quarkus"))
  implementation(project(":nessie-rest-services"))
  implementation(project(":nessie-versioned-spi"))
  implementation(project(":nessie-versioned-persist-adapter"))
  implementation(project(":nessie-versioned-persist-store"))
  implementation(libs.nessie.ui)

  implementation(enforcedPlatform(libs.quarkus.bom))
  implementation(enforcedPlatform(libs.quarkus.amazon.services.bom))
  implementation("org.jboss.resteasy:resteasy-core-spi")
  implementation("io.quarkus:quarkus-resteasy")
  implementation("io.quarkus:quarkus-resteasy-jackson")
  implementation("io.quarkus:quarkus-reactive-routes")
  implementation("io.quarkus:quarkus-elytron-security-properties-file")
  implementation("io.quarkus:quarkus-smallrye-health")
  implementation("io.quarkus:quarkus-oidc")
  implementation("io.quarkus:quarkus-smallrye-openapi")
  implementation("io.quarkus:quarkus-micrometer")
  implementation("io.quarkus:quarkus-core-deployment")
  implementation("io.quarkus:quarkus-opentelemetry")
  implementation(libs.quarkus.logging.sentry)
  implementation("io.smallrye:smallrye-open-api-jaxrs")
  implementation("io.micrometer:micrometer-registry-prometheus")

  implementation(platform(libs.cel.bom))
  implementation("org.projectnessie.cel:cel-tools")
  implementation("org.projectnessie.cel:cel-jackson")

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
  implementation("io.opentelemetry:opentelemetry-opentracing-shim")
  implementation("io.opentelemetry.instrumentation:opentelemetry-micrometer-1.5")
  implementation(libs.opentracing.util)

  openapiSource(project(":nessie-model", "openapiSource"))

  testFixturesApi(project(":nessie-client"))
  testFixturesApi(project(":nessie-client-testextension"))
  testFixturesApi(project(":nessie-jaxrs-tests"))
  testFixturesApi(project(":nessie-quarkus-tests"))
  testFixturesApi(project(":nessie-events-api"))
  testFixturesApi(project(":nessie-events-spi"))
  testFixturesApi(project(":nessie-events-service"))
  testFixturesImplementation(project(":nessie-versioned-spi"))
  testFixturesImplementation(project(":nessie-versioned-tests"))
  testFixturesImplementation(project(":nessie-versioned-persist-adapter"))
  testFixturesImplementation(project(":nessie-versioned-persist-store"))
  testFixturesImplementation(project(":nessie-versioned-persist-tests"))
  testFixturesApi(project(":nessie-versioned-storage-common"))
  testFixturesApi(project(":nessie-versioned-storage-store"))
  testFixturesImplementation(project(":nessie-versioned-storage-testextension"))
  testFixturesApi(enforcedPlatform(libs.quarkus.bom))
  testFixturesImplementation("com.fasterxml.jackson.core:jackson-annotations")
  testFixturesApi("io.quarkus:quarkus-rest-client")
  testFixturesApi("io.quarkus:quarkus-test-security")
  testFixturesApi("io.quarkus:quarkus-test-oidc-server")
  testFixturesImplementation(libs.guava)
  testFixturesImplementation(libs.microprofile.openapi)
  testFixturesImplementation(libs.awaitility)

  testFixturesApi(platform(libs.junit.bom))
  testFixturesApi(libs.bundles.junit.testing)

  intTestImplementation("io.quarkus:quarkus-test-keycloak-server")
  intTestImplementation(project(":nessie-keycloak-testcontainer"))
  intTestImplementation(libs.keycloak.admin.client)
  intTestRuntimeOnly(platform(libs.testcontainers.bom))
  intTestRuntimeOnly("org.testcontainers:cassandra")
  intTestRuntimeOnly("org.testcontainers:postgresql")
  intTestRuntimeOnly("org.testcontainers:mongodb")
  intTestImplementation(project(":nessie-keycloak-testcontainer"))
  intTestRuntimeOnly(project(":nessie-nessie-testcontainer"))
  // Keycloak-admin-client depends on Resteasy.
  // Need to bump Resteasy, because Resteasy < 6.2.4 clashes with our Jackson version management and
  // cause non-existing jackson versions like 2.15.2-jakarta, which then lets the build fail.
  intTestImplementation(platform(libs.resteasy.bom))
}

val pullOpenApiSpec by tasks.registering(Sync::class)

pullOpenApiSpec.configure {
  destinationDir = openApiSpecDir
  from(project.objects.property(Configuration::class).value(openapiSource))
}

val openApiSpecDir = buildDir.resolve("openapi-extra")

quarkus {
  quarkusBuildProperties.put("quarkus.package.type", quarkusPackageType())
  quarkusBuildProperties.put(
    "quarkus.smallrye-openapi.store-schema-directory",
    buildDir.resolve("openapi").toString()
  )
  quarkusBuildProperties.put(
    "quarkus.smallrye-openapi.additional-docs-directory",
    openApiSpecDir.toString()
  )
}

val quarkusBuild = tasks.named<QuarkusBuild>("quarkusBuild")

quarkusBuild.configure { dependsOn(pullOpenApiSpec) }

tasks.withType<Test>().configureEach {
  systemProperty(
    "it.nessie.container.postgres.tag",
    System.getProperty("it.nessie.container.postgres.tag", libs.versions.postgresContainerTag.get())
  )
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
  tasks.named(name).configure { dependsOn(tasks.named("compileQuarkusGeneratedSourcesJava")) }
}

listOf("checkstyleTest", "compileTestJava").forEach { name ->
  tasks.named(name).configure { dependsOn(tasks.named("compileQuarkusTestGeneratedSourcesJava")) }
}

// Testcontainers is not supported on Windows :(
if (Os.isFamily(Os.FAMILY_WINDOWS)) {
  tasks.named<Test>("intTest").configure { this.enabled = false }
}

// Issue w/ testcontainers/podman in GH workflows :(
if (Os.isFamily(Os.FAMILY_MAC) && System.getenv("CI") != null) {
  tasks.named<Test>("intTest").configure { this.enabled = false }
}

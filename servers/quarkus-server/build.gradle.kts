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
  implementation(project(":nessie-quarkus-auth"))
  implementation(project(":nessie-quarkus-common"))
  implementation(project(":nessie-events-quarkus"))
  implementation(project(":nessie-services-config"))
  implementation(project(":nessie-rest-common"))
  implementation(project(":nessie-rest-services"))
  implementation(project(":nessie-versioned-spi"))
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
  implementation("org.projectnessie.cel:cel-standalone")

  if (project.hasProperty("k8s")) {
    /*
     * Use the k8s project property to generate manifest files for K8s & Minikube, which will be
     * located under target/kubernetes.
     * See also https://quarkus.io/guides/deploying-to-kubernetes for additional details
     */
    implementation("io.quarkus:quarkus-kubernetes")
    implementation("io.quarkus:quarkus-minikube")
  }

  openapiSource(project(":nessie-model", "openapiSource"))

  testFixturesApi(project(":nessie-client"))
  testFixturesApi(testFixtures(project(":nessie-client")))
  testFixturesApi(project(":nessie-client-testextension"))
  testFixturesApi(project(":nessie-jaxrs-tests"))
  testFixturesApi(project(":nessie-quarkus-auth"))
  testFixturesApi(project(":nessie-quarkus-common"))
  testFixturesApi(project(":nessie-quarkus-tests"))
  testFixturesApi(project(":nessie-events-api"))
  testFixturesApi(project(":nessie-events-spi"))
  testFixturesApi(project(":nessie-events-service"))
  testFixturesImplementation(project(":nessie-versioned-spi"))
  testFixturesImplementation(project(":nessie-versioned-tests"))
  testFixturesApi(project(":nessie-versioned-storage-common"))
  testFixturesApi(project(":nessie-versioned-storage-store"))
  testFixturesImplementation(project(":nessie-versioned-storage-testextension")) {
    // Needed to avoid dependency resolution error:
    // Module 'com.google.guava:guava' ... rejected: 'com.google.guava:listenablefuture:1.0' also
    // provided by
    // [com.google.guava:listenablefuture:9999.0-empty-to-avoid-conflict-with-guava(compile)]
    exclude("com.google.guava")
  }
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
}

val pullOpenApiSpec by tasks.registering(Sync::class)

val openApiSpecDir = layout.buildDirectory.dir("openapi-extra")

pullOpenApiSpec.configure {
  destinationDir = openApiSpecDir.get().asFile
  from(openapiSource) { include("openapi.yaml") }
}

quarkus {
  quarkusBuildProperties.put("quarkus.package.type", quarkusPackageType())
  quarkusBuildProperties.put(
    "quarkus.smallrye-openapi.store-schema-directory",
    layout.buildDirectory.asFile.map { it.resolve("openapi") }.get().toString()
  )
  quarkusBuildProperties.put(
    "quarkus.smallrye-openapi.additional-docs-directory",
    openApiSpecDir.get().toString()
  )
  quarkusBuildProperties.put("quarkus.smallrye-openapi.info-version", project.version.toString())
  quarkusBuildProperties.put("quarkus.smallrye-openapi.auto-add-security", "false")
  // Pull manifest attributes from the "main" `jar` task to get the
  // release-information into the jars generated by Quarkus.
  quarkusBuildProperties.putAll(
    provider {
      tasks
        .named("jar", Jar::class.java)
        .get()
        .manifest
        .attributes
        .map { e -> "quarkus.package.manifest.attributes.\"${e.key}\"" to e.value.toString() }
        .toMap()
    }
  )
}

val quarkusAppPartsBuild = tasks.named("quarkusAppPartsBuild")

quarkusAppPartsBuild.configure {
  dependsOn(pullOpenApiSpec)
  inputs.files(openapiSource)
}

val quarkusBuild = tasks.named<QuarkusBuild>("quarkusBuild")

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

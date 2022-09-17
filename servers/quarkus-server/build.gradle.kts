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

plugins {
  `java-library`
  `maven-publish`
  signing
  id("io.quarkus")
  `nessie-conventions`
}

extra["maven.name"] = "Nessie - Quarkus Server"

val quarkusRunner by
  configurations.creating {
    description = "Used to reference the generated runner-jar (either fast-jar or uber-jar)"
  }

val openapiSource by
  configurations.creating { description = "Used to reference OpenAPI spec files" }

dependencies {
  implementation(platform(rootProject))
  implementation(platform(project(":nessie-deps-persist")))
  implementation(platform(project(":nessie-deps-quarkus")))
  implementation(enforcedPlatform("io.quarkus:quarkus-bom"))
  implementation(enforcedPlatform("io.quarkus.platform:quarkus-amazon-services-bom"))
  implementation(platform("org.projectnessie.cel:cel-bom"))

  implementation(project(":nessie-model"))
  implementation(project(":nessie-services"))
  implementation(project(":nessie-quarkus-common"))
  implementation(project(":nessie-rest-services"))
  implementation(project(":nessie-versioned-spi"))
  implementation(project(":nessie-versioned-persist-adapter"))
  implementation(project(":nessie-versioned-persist-store"))
  implementation(project(":nessie-ui"))
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
  implementation("io.quarkus:quarkus-smallrye-opentracing")
  implementation("io.quarkus:quarkus-container-image-jib")
  implementation("io.quarkiverse.loggingsentry:quarkus-logging-sentry")
  implementation("io.smallrye:smallrye-open-api-jaxrs")
  implementation("io.micrometer:micrometer-registry-prometheus")
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

  openapiSource(project(":nessie-model", "openapiSource"))

  testImplementation(platform(project(":nessie-deps-testing")))
  testImplementation(platform("org.junit:junit-bom"))

  testImplementation(project(":nessie-client"))
  testImplementation(project(":nessie-jaxrs-tests"))
  testImplementation(project(":nessie-quarkus-tests"))
  testImplementation(project(":nessie-versioned-tests"))
  testImplementation(project(":nessie-versioned-persist-tests"))
  testImplementation("io.quarkus:quarkus-rest-client")
  testImplementation("io.quarkus:quarkus-test-security")
  testImplementation("io.quarkus:quarkus-test-oidc-server")
  testImplementation("io.quarkus:quarkus-jacoco")

  testImplementation("org.assertj:assertj-core")
  testImplementation("org.mockito:mockito-core")
  testImplementation("org.junit.jupiter:junit-jupiter-api")
  testImplementation("org.junit.jupiter:junit-jupiter-params")
  testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine")
}

preferJava11()

val openApiSpecDir = project.buildDir.resolve("openapi-extra").relativeTo(project.projectDir)

project.extra["quarkus.smallrye-openapi.store-schema-directory"] =
  "${project.buildDir.relativeTo(project.projectDir)}/openapi"

project.extra["quarkus.smallrye-openapi.additional-docs-directory"] = "$openApiSpecDir"

project.extra["quarkus.package.type"] =
  if (withUberJar()) "uber-jar" else if (project.hasProperty("native")) "native" else "fast-jar"

// TODO remove the whole block
quarkus { setFinalName("${project.name}-${project.version}") }

val useDocker = project.hasProperty("docker")

val pullOpenApiSpec by
  tasks.registering(Sync::class) {
    destinationDir = openApiSpecDir
    from(project.objects.property(Configuration::class).value(openapiSource))
  }

val quarkusBuild =
  tasks.named<QuarkusBuild>("quarkusBuild") {
    dependsOn(pullOpenApiSpec)
    inputs.property("quarkus.package.type", project.extra["quarkus.package.type"])
    inputs.property("final.name", quarkus.finalName())
    inputs.property("builder-image", dependencyVersion("quarkus.builder-image"))
    inputs.property("container-build", useDocker)
    if (useDocker) {
      // Use the "docker" profile to just build the Docker container image when the native image's
      // been built
      nativeArgs { "container-build" to true }
    }
    nativeArgs { "builder-image" to dependencyVersion("quarkus.builder-image") }
    doFirst {
      // THIS IS A WORKAROUND! the nativeArgs{} thing above doesn't really work
      System.setProperty("quarkus.native.builder-image", dependencyVersion("quarkus.builder-image"))
      if (useDocker) {
        System.setProperty("quarkus.native.container-build", "true")
        System.setProperty("quarkus.container-image.build", "true")
      }
    }
  }

tasks.withType<Test>().configureEach {
  if (project.hasProperty("native")) {
    systemProperty("native.image.path", quarkusBuild.get().nativeRunner)
  }
  systemProperty("quarkus.container-image.build", useDocker)
  systemProperty("quarkus.smallrye.jwt.enabled", "true")
  // TODO requires adjusting the tests - systemProperty("quarkus.http.test-port", "0") -  set this
  //  property in application.properties
}

tasks.named<Test>("intTest") { filter { excludeTestsMatching("ITNative*") } }

// Expose runnable jar via quarkusRunner configuration for integration-tests that require the
// server.
artifacts {
  add(
    quarkusRunner.name,
    provider {
      if (withUberJar()) quarkusBuild.get().runnerJar
      else quarkusBuild.get().fastJar.resolve("quarkus-run.jar")
    }
  ) {
    builtBy(quarkusBuild)
  }
}

// Add the uber-jar, if built, to the Maven publication
if (withUberJar()) {
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

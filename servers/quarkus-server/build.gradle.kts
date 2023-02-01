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
import io.quarkus.gradle.tasks.QuarkusGenerateCode
import org.apache.tools.ant.taskdefs.condition.Os

plugins {
  `java-library`
  `maven-publish`
  signing
  alias(libs.plugins.quarkus)
  `nessie-conventions`
}

extra["maven.name"] = "Nessie - Quarkus Server"

val quarkusRunner by
  configurations.creating {
    description = "Used to reference the generated runner-jar (either fast-jar or uber-jar)"
  }

val openapiSource by
  configurations.creating { description = "Used to reference OpenAPI spec files" }

val jacocoRuntime by configurations.creating { description = "Jacoco task runtime" }

dependencies {
  implementation(project(":nessie-model"))
  implementation(project(":nessie-services"))
  implementation(project(":nessie-quarkus-common"))
  implementation(project(":nessie-rest-services"))
  implementation(project(":nessie-versioned-spi"))
  implementation(project(":nessie-versioned-persist-adapter"))
  implementation(project(":nessie-versioned-persist-store"))
  implementation(project(":nessie-ui"))

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
  implementation(libs.quarkus.opentelemetry)
  implementation("io.quarkus:quarkus-container-image-jib")
  implementation(libs.quarkus.logging.sentry)
  implementation("io.smallrye:smallrye-open-api-jaxrs")
  implementation("io.micrometer:micrometer-registry-prometheus")

  implementation(platform(libs.cel.bom))
  implementation(libs.cel.tools)
  implementation(libs.cel.jackson)

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

  testImplementation(project(":nessie-client"))
  testImplementation(project(":nessie-client-testextension"))
  testImplementation(project(":nessie-jaxrs-tests"))
  testImplementation(project(":nessie-quarkus-tests"))
  testImplementation(project(":nessie-versioned-tests"))
  testImplementation(project(":nessie-versioned-persist-tests"))
  testImplementation("io.quarkus:quarkus-rest-client")
  testImplementation("io.quarkus:quarkus-test-security")
  testImplementation("io.quarkus:quarkus-test-oidc-server")
  testImplementation("io.quarkus:quarkus-jacoco")

  testImplementation(platform(libs.junit.bom))
  testImplementation(libs.bundles.junit.testing)
  testRuntimeOnly(libs.junit.jupiter.engine)

  jacocoRuntime(libs.jacoco.report)
  jacocoRuntime(libs.jacoco.ant)
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
    inputs.property("container-build", useDocker)
    val quarkusBuilderImage = libs.versions.quarkusBuilderImage.get()
    inputs.property("builder-image", quarkusBuilderImage)
    if (useDocker) {
      // Use the "docker" profile to just build the Docker container image when the native image's
      // been built
      nativeArgs { "container-build" to true }
    }
    nativeArgs { "builder-image" to quarkusBuilderImage }
    doFirst {
      // THIS IS A WORKAROUND! the nativeArgs{} thing above doesn't really work
      System.setProperty("quarkus.native.builder-image", quarkusBuilderImage)
      if (useDocker) {
        System.setProperty("quarkus.native.container-build", "true")
        System.setProperty("quarkus.container-image.build", "true")
      }
    }
  }

tasks.withType<QuarkusGenerateCode>().configureEach { outputs.cacheIf { true } }

val prepareJacocoReport by
  tasks.registering {
    doFirst {
      // Must delete the Jacoco data file before running tests, because
      // quarkus.jacoco.reuse-data-file=true in application.properties.
      file("${project.buildDir}/jacoco-quarkus.exec").delete()
      val reportDir = file("${project.buildDir}/jacoco-report")
      delete { delete(reportDir) }
      reportDir.mkdirs()
    }
  }

val jacocoReport by
  tasks.registering(JacocoReport::class) {
    dependsOn(
      tasks.named("compileIntegrationTestJava"),
      tasks.named("compileNativeTestJava"),
      tasks.named("compileQuarkusGeneratedSourcesJava")
    )
    executionData.from(file("${project.buildDir}/jacoco-quarkus.exec"))
    jacocoClasspath = jacocoRuntime
    classDirectories.from(layout.buildDirectory.dir("classes"))
    sourceDirectories
      .from(layout.projectDirectory.dir("src/main/java"))
      .from(layout.projectDirectory.dir("src/test/java"))
    reports {
      xml.required.set(true)
      xml.outputLocation.set(layout.buildDirectory.file("jacoco-report/jacoco.xml"))
      csv.required.set(true)
      csv.outputLocation.set(layout.buildDirectory.file("jacoco-report/jacoco.csv"))
      html.required.set(true)
      html.outputLocation.set(layout.buildDirectory.dir("jacoco-report"))
    }
  }

tasks.withType<Test>().configureEach {
  dependsOn(prepareJacocoReport)
  finalizedBy(jacocoReport)

  if (project.hasProperty("native")) {
    systemProperty("native.image.path", quarkusBuild.get().nativeRunner)
  }
  systemProperty("quarkus.container-image.build", useDocker)
  systemProperty("quarkus.smallrye.jwt.enabled", "true")
  systemProperty(
    "it.nessie.container.postgres.tag",
    System.getProperty("it.nessie.container.postgres.tag", libs.versions.postgresContainerTag.get())
  )
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

// Testcontainers is not supported on Windows :(
if (Os.isFamily(Os.FAMILY_WINDOWS)) {
  tasks.withType<Test>().configureEach { this.enabled = false }
}

/*
 * Copyright (C) 2024 Dremio
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

/*
 * Copyright (C) 2024 Dremio
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

import java.io.ByteArrayOutputStream
import org.apache.tools.ant.filters.ReplaceTokens

plugins {
  alias(libs.plugins.quarkus)
  id("nessie-conventions-quarkus")
  id("nessie-license-report")
}

extra["maven.name"] = "Nessie - Kubernetes Operator"

dependencies {
  implementation(enforcedPlatform(libs.quarkus.bom))
  implementation(platform(libs.quarkus.operator.sdk.bom))
  implementation("io.quarkiverse.operatorsdk:quarkus-operator-sdk")
  implementation("io.quarkiverse.operatorsdk:quarkus-operator-sdk-bundle-generator")
  implementation("io.quarkus:quarkus-micrometer-registry-prometheus")
  implementation("io.quarkus:quarkus-container-image-jib")

  implementation("org.bouncycastle:bcpkix-jdk18on")

  compileOnly(libs.sundr.builder.annotations)
  compileOnly("io.fabric8:generator-annotations")

  annotationProcessor(enforcedPlatform(libs.quarkus.bom))
  annotationProcessor(libs.sundr.builder.annotations)
  // see https://github.com/sundrio/sundrio/issues/104
  annotationProcessor("io.fabric8:kubernetes-client")

  testFixturesApi(enforcedPlatform(libs.quarkus.bom))
  testFixturesApi(platform(libs.quarkus.operator.sdk.bom))
  testFixturesApi(platform(libs.junit.bom))
  testFixturesApi("io.quarkus:quarkus-junit5")
  testFixturesApi("io.fabric8:openshift-client")
  testFixturesApi(libs.bundles.junit.testing)
  testFixturesApi(libs.awaitility)

  testImplementation("io.quarkus:quarkus-test-kubernetes-client")
  testImplementation("io.fabric8:kubernetes-server-mock")

  intTestImplementation(platform(libs.testcontainers.bom))
  intTestImplementation(project(":nessie-client"))
  intTestImplementation("org.testcontainers:k3s")
  intTestImplementation("org.testcontainers:mongodb")
  intTestImplementation("org.testcontainers:postgresql")
  intTestImplementation("org.testcontainers:cassandra")
  intTestImplementation(platform(libs.cassandra.driver.bom))
  intTestImplementation("com.datastax.oss:java-driver-core")
  intTestImplementation(project(":nessie-keycloak-testcontainer"))
  intTestImplementation(project(":nessie-container-spec-helper"))

  intTestCompileOnly(libs.microprofile.openapi)
  intTestCompileOnly(libs.immutables.value.annotations)
}

listOf("javadoc", "sourcesJar").forEach { name ->
  tasks.named(name).configure { dependsOn(tasks.named("compileQuarkusGeneratedSourcesJava")) }
}

listOf("checkstyleTest", "compileTestJava").forEach { name ->
  tasks.named(name).configure { dependsOn(tasks.named("compileQuarkusTestGeneratedSourcesJava")) }
}

tasks.named<ProcessResources>("processTestResources").configure {
  inputs.property("projectVersion", project.version)
  filter(ReplaceTokens::class, mapOf("tokens" to mapOf("projectVersion" to project.version)))
}

tasks.named<ProcessResources>("processIntTestResources").configure {
  inputs.property("projectVersion", project.version)
  filter(ReplaceTokens::class, mapOf("tokens" to mapOf("projectVersion" to project.version)))
}

tasks.named("quarkusAppPartsBuild").configure {
  outputs.dir(project.layout.buildDirectory.dir("helm"))
  outputs.dir(project.layout.buildDirectory.dir("kubernetes"))
  outputs.dir(project.layout.buildDirectory.dir("bundle"))
}

tasks.named("quarkusDependenciesBuild").configure { dependsOn("processJandexIndex") }

tasks.named<Test>("intTest").configure {
  dependsOn(buildNessieServerTestImage)
  dependsOn(buildNessieGcTestImage)
  // Required to install the CRDs during integration tests
  val crdsDir = project.layout.buildDirectory.dir("kubernetes").get().asFile.toString()
  systemProperty("nessie.crds.dir", crdsDir)
  // Required for Ingress tests
  systemProperty("jdk.httpclient.allowRestrictedHeaders", "host")
}

// Builds the Nessie server image to use in integration tests.
// The image will then be loaded into the running K3S cluster,
// see K3sContainerLifecycleManager.
val buildNessieServerTestImage by
  tasks.registering(Exec::class) {
    dependsOn(":nessie-quarkus:quarkusBuild")
    workingDir = project.layout.projectDirectory.asFile.parentFile
    executable =
      which("docker")
        ?: which("podman")
        ?: throw IllegalStateException("Neither docker nor podman found on the system")
    args(
      "build",
      "--file",
      "tools/dockerbuild/docker/Dockerfile-server",
      "--tag",
      "projectnessie/nessie-test-server:" + project.version,
      "servers/quarkus-server"
    )
  }

// Builds the Nessie GC image to use in integration tests.
// The image will then be loaded into the running K3S cluster,
// see K3sContainerLifecycleManager.
val buildNessieGcTestImage by
  tasks.registering(Exec::class) {
    dependsOn(":nessie-gc-tool:shadowJar")
    workingDir = project.layout.projectDirectory.asFile.parentFile
    executable =
      which("docker")
        ?: which("podman")
        ?: throw IllegalStateException("Neither docker nor podman found on the system")
    args(
      "build",
      "--file",
      "tools/dockerbuild/docker/Dockerfile-gctool",
      "--tag",
      "projectnessie/nessie-test-gc:" + project.version,
      "gc/gc-tool"
    )
  }

private fun which(command: String): String? {
  val stdout = ByteArrayOutputStream()
  val result = exec {
    isIgnoreExitValue = true
    standardOutput = stdout
    commandLine("which", command)
  }
  return if (result.exitValue == 0) "$stdout".trim() else null
}

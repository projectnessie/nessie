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

import org.apache.tools.ant.filters.ReplaceTokens

plugins {
  alias(libs.plugins.quarkus)
  id("nessie-conventions-quarkus")
  id("nessie-jacoco")
}

extra["maven.name"] = "Nessie - Kubernetes Operator"

dependencies {
  implementation(enforcedPlatform(libs.quarkus.bom))
  implementation(enforcedPlatform(libs.quarkus.operator.sdk.bom))
  implementation("io.quarkiverse.operatorsdk:quarkus-operator-sdk")
  implementation("io.quarkiverse.operatorsdk:quarkus-operator-sdk-bundle-generator")
  implementation("io.quarkus:quarkus-micrometer-registry-prometheus")
  implementation("io.quarkus:quarkus-container-image-jib")

  runtimeOnly("org.bouncycastle:bcpkix-jdk18on")

  compileOnly("io.sundr:builder-annotations:0.103.1")
  compileOnly("io.fabric8:generator-annotations")

  annotationProcessor(enforcedPlatform(libs.quarkus.bom))
  annotationProcessor("io.sundr:builder-annotations:0.103.1")
  // see https://github.com/sundrio/sundrio/issues/104
  annotationProcessor("io.fabric8:kubernetes-client")

  testFixturesApi(enforcedPlatform(libs.quarkus.bom))
  testFixturesApi(enforcedPlatform(libs.quarkus.operator.sdk.bom))
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

  intTestCompileOnly(libs.microprofile.openapi)
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
  // Caching is disabled because the task does not properly handle the following outputs:
  // - build/kubernetes/*
  // - build/helm/*
  // - build/bundle/*
  outputs.cacheIf { false }
}

tasks.named<Test>("intTest").configure {
  // Required to install the CRDs during integration tests
  systemProperty(
    "nessie.crds.dir",
    project.layout.buildDirectory.dir("kubernetes").get().asFile.toString()
  )
  // Required for Ingress tests
  systemProperty("jdk.httpclient.allowRestrictedHeaders", "host")
}

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
    .version(
      libs.plugins.quarkus.asProvider().map {
        System.getProperty("quarkus.custom.version", it.version.requiredVersion)
      }
    )
  id("nessie-conventions-quarkus")
  id("nessie-license-report")
}

publishingHelper { mavenName = "Nessie - Quarkus Server" }

val quarkusRunner by
  configurations.creating {
    description = "Used to reference the generated runner-jar (either fast-jar or uber-jar)"
  }

val openapiSource by
  configurations.creating { description = "Used to reference OpenAPI spec files" }

val versionIceberg = libs.versions.iceberg.get()

dnsjavaDowngrade()

cassandraDriverTweak()

dependencies {
  implementation(project(":nessie-client"))
  implementation(project(":nessie-model"))
  implementation(project(":nessie-services"))
  implementation(project(":nessie-services-config"))
  implementation(project(":nessie-quarkus-authn"))
  implementation(project(":nessie-quarkus-authz"))
  implementation(project(":nessie-quarkus-catalog"))
  implementation(project(":nessie-quarkus-common"))
  implementation(project(":nessie-quarkus-config"))
  implementation(project(":nessie-quarkus-distcache"))
  implementation(project(":nessie-quarkus-secrets"))
  implementation(project(":nessie-quarkus-rest"))
  implementation(project(":nessie-events-quarkus"))
  implementation(project(":nessie-rest-common"))
  implementation(project(":nessie-rest-services"))
  implementation(project(":nessie-versioned-spi"))
  implementation(project(":nessie-notice"))
  implementation(project(":nessie-versioned-storage-jdbc"))
  implementation(project(":nessie-versioned-storage-jdbc2"))
  implementation(libs.nessie.ui)

  // Nessie internal Quarkus extension, currently only disables "non-indexed classes" (Jandex)
  // warnings.
  compileOnly(project(":nessie-quarkus-ext-deployment"))
  implementation(project(":nessie-quarkus-ext"))

  implementation(quarkusPlatform(project))
  implementation(quarkusExtension(project, "amazon-services"))
  implementation("io.quarkus:quarkus-rest")
  implementation("io.quarkus:quarkus-rest-jackson")
  implementation("io.quarkus:quarkus-reactive-routes")
  implementation("io.quarkus:quarkus-hibernate-validator")
  implementation("io.quarkus:quarkus-smallrye-context-propagation")
  implementation("io.quarkus:quarkus-smallrye-health")
  implementation("io.quarkus:quarkus-smallrye-openapi")
  implementation("io.quarkus:quarkus-security")
  implementation("io.quarkus:quarkus-elytron-security-properties-file")
  implementation("io.quarkus:quarkus-oidc")
  implementation("io.quarkus:quarkus-micrometer")
  implementation("io.quarkus:quarkus-opentelemetry")
  implementation("io.quarkus:quarkus-logging-json")
  implementation(libs.quarkus.logging.sentry)
  implementation("io.smallrye:smallrye-open-api-jaxrs")
  implementation("io.micrometer:micrometer-registry-prometheus-simpleclient")

  implementation(platform(libs.cel.bom))
  implementation("org.projectnessie.cel:cel-standalone")

  implementation(libs.guava)

  compileOnly(libs.microprofile.openapi)

  compileOnly(project(":nessie-immutables-std"))
  annotationProcessor(project(":nessie-immutables-std", configuration = "processor"))

  openapiSource(project(":nessie-model")) { isTransitive = false }

  testFixturesApi(platform(libs.junit.bom))
  testFixturesApi(libs.bundles.junit.testing)

  testFixturesApi(project(":nessie-client"))
  testFixturesApi(project(":nessie-model"))
  testFixturesApi(testFixtures(project(":nessie-client")))
  testFixturesApi(project(":nessie-client-testextension"))
  testFixturesApi(project(":nessie-jaxrs-tests"))
  testFixturesApi(project(":nessie-quarkus-authn"))
  testFixturesApi(project(":nessie-quarkus-authz"))
  testFixturesApi(project(":nessie-quarkus-common"))
  testFixturesApi(project(":nessie-quarkus-tests")) {
    // Don't use resteasy-classic (and prevent the Quarkus warning)
    exclude(group = "org.jboss.resteasy")
  }
  testFixturesApi(project(":nessie-quarkus-config"))
  testFixturesApi(project(":nessie-quarkus-tests"))
  testFixturesApi(project(":nessie-catalog-files-api"))
  testFixturesApi(project(":nessie-catalog-files-impl"))
  testFixturesApi(project(":nessie-catalog-service-common"))
  testFixturesApi(project(":nessie-catalog-service-config"))
  testFixturesApi(project(":nessie-events-api"))
  testFixturesApi(project(":nessie-events-spi"))
  testFixturesApi(project(":nessie-events-service"))
  testFixturesImplementation(project(":nessie-versioned-spi"))
  testFixturesImplementation(project(":nessie-versioned-tests"))
  testFixturesApi(project(":nessie-versioned-storage-common"))
  testFixturesApi(project(":nessie-versioned-storage-cache"))
  testFixturesApi(project(":nessie-versioned-storage-store"))
  testFixturesImplementation(project(":nessie-versioned-storage-testextension")) {
    // Needed to avoid dependency resolution error:
    // Module 'com.google.guava:guava' ... rejected: 'com.google.guava:listenablefuture:1.0' also
    // provided by
    // [com.google.guava:listenablefuture:9999.0-empty-to-avoid-conflict-with-guava(compile)]
    exclude("com.google.guava")
  }
  testFixturesApi(quarkusPlatform(project))
  testFixturesImplementation("com.fasterxml.jackson.core:jackson-annotations")
  testFixturesApi("io.quarkus:quarkus-test-security")
  testFixturesApi("io.quarkus:quarkus-test-oidc-server")
  testFixturesImplementation(libs.guava)
  testFixturesImplementation(libs.microprofile.openapi)
  testFixturesImplementation(libs.awaitility)
  testFixturesApi(libs.jakarta.validation.api)
  testFixturesApi(testFixtures(project(":nessie-quarkus-common")))

  testFixturesApi(platform(libs.testcontainers.bom))
  testFixturesApi("org.testcontainers:testcontainers")
  testFixturesApi(project(":nessie-keycloak-testcontainer")) {
    // Don't use resteasy-classic (and prevent the Quarkus warning)
    exclude(group = "org.jboss.resteasy")
  }
  testFixturesApi("org.testcontainers:testcontainers-localstack")
  testFixturesApi("org.testcontainers:testcontainers-vault")
  testFixturesApi(project(":nessie-keycloak-testcontainer"))
  testFixturesApi(project(":nessie-azurite-testcontainer"))
  testFixturesApi(project(":nessie-gcs-testcontainer"))
  testFixturesApi(project(":nessie-minio-testcontainer"))
  testFixturesApi(project(":nessie-trino-testcontainer"))
  testFixturesApi(project(":nessie-object-storage-mock"))
  testFixturesApi(project(":nessie-catalog-format-iceberg"))
  testFixturesApi(project(":nessie-catalog-format-iceberg-fixturegen"))
  testFixturesApi(project(":nessie-container-spec-helper"))
  testFixturesApi(project(":nessie-catalog-secrets-api"))

  testFixturesApi(platform(libs.awssdk.bom))
  testFixturesApi("software.amazon.awssdk:secretsmanager")
  testFixturesApi(libs.quarkus.vault.deployment)

  testFixturesApi(enforcedPlatform(libs.quarkus.azure.services.bom))
  testFixturesApi("io.quarkiverse.azureservices:quarkus-azure-keyvault")

  testFixturesApi(platform("org.apache.iceberg:iceberg-bom:$versionIceberg"))
  testFixturesApi("org.apache.iceberg:iceberg-core")
  testFixturesApi("org.apache.iceberg:iceberg-bundled-guava")
  testFixturesApi("org.apache.iceberg:iceberg-aws")
  testFixturesApi("org.apache.iceberg:iceberg-gcp")
  testFixturesApi("org.apache.iceberg:iceberg-azure")
  testFixturesApi("org.apache.iceberg:iceberg-api:$versionIceberg:tests")
  testFixturesApi("org.apache.iceberg:iceberg-core:$versionIceberg:tests")
  testFixturesApi(libs.hadoop.common) {
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

  // These two testFixturesRuntimeOnly are here to avoid the 'Failed to index javax...: Class does
  // not exist in ClassLoader' warnings
  testFixturesRuntimeOnly(libs.javax.validation.api)
  testFixturesRuntimeOnly(libs.javax.ws.rs)

  testFixturesCompileOnly(libs.microprofile.openapi)

  testFixturesCompileOnly(project(":nessie-immutables"))
  testCompileOnly(project(":nessie-immutables"))
  testAnnotationProcessor(project(":nessie-immutables", configuration = "processor"))
  intTestCompileOnly(project(":nessie-immutables"))
  intTestAnnotationProcessor(project(":nessie-immutables", configuration = "processor"))

  intTestImplementation(enforcedPlatform(libs.testcontainers.bom))
  intTestImplementation("io.quarkus:quarkus-test-keycloak-server")
  intTestImplementation(project(":nessie-keycloak-testcontainer"))
  intTestImplementation(libs.lowkey.vault.testcontainers)

  intTestImplementation(platform(libs.awssdk.bom))
  intTestImplementation("software.amazon.awssdk:s3")
  intTestImplementation("software.amazon.awssdk:sts")

  intTestCompileOnly(libs.immutables.value.annotations)
}

val pullOpenApiSpec by
  tasks.registering(Sync::class) {
    inputs.files(openapiSource)
    destinationDir = layout.buildDirectory.dir("resources/openapi").get().asFile
    from(provider { zipTree(openapiSource.singleFile) }) {
      include("META-INF/openapi/**")
      eachFile { path = "META-INF/resources/nessie-openapi/${file.name}" }
    }
  }

sourceSets.named("main") { resources.srcDir(pullOpenApiSpec) }

tasks.named("processResources") { dependsOn(pullOpenApiSpec) }

tasks.withType(Test::class.java).configureEach {
  // Needed by org.projectnessie.server.catalog.IcebergResourceLifecycleManager to "clean up"
  // the shutdown hooks. See there for more information.
  jvmArgs("--add-opens=java.base/java.lang=ALL-UNNAMED")
  // Don't use Netty's epoll/iouring/kqueue mechanisms, because some libraryies, especially
  // the Azure SDK, do never clean up resources (threads), which lead to OOMs in Quarkus tests
  // (Quarkus uses a separate class loader for every test class).
  systemProperty("io.netty.transport.noNative", "true")
}

quarkus {
  quarkusBuildProperties.put("quarkus.package.type", quarkusPackageType())
  // Pull manifest attributes from the "main" `jar` task to get the
  // release-information into the jars generated by Quarkus.
  quarkusBuildProperties.putAll(
    provider {
      tasks
        .named("jar", Jar::class.java)
        .get()
        .manifest
        .attributes
        .map { e -> "quarkus.package.jar.manifest.attributes.\"${e.key}\"" to e.value.toString() }
        .toMap()
    }
  )
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
    },
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

// Testcontainers is not supported on Windows :(
if (Os.isFamily(Os.FAMILY_WINDOWS)) {
  tasks.named<Test>("intTest").configure { this.enabled = false }
}

// Issue w/ testcontainers/podman in GH workflows :(
if (Os.isFamily(Os.FAMILY_MAC) && System.getenv("CI") != null) {
  tasks.named<Test>("intTest").configure { this.enabled = false }
}

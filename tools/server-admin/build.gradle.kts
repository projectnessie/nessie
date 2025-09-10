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

publishingHelper { mavenName = "Nessie - Server Admin Tool" }

val quarkusRunner by
  configurations.creating {
    description = "Used to reference the generated runner-jar (either fast-jar or uber-jar)"
  }

cassandraDriverTweak()

dependencies {
  implementation(project(":nessie-quarkus-common"))
  implementation(project(":nessie-quarkus-config"))
  implementation(project(":nessie-services"))
  implementation(project(":nessie-services-config"))
  implementation(project(":nessie-server-store"))
  implementation(project(":nessie-versioned-spi"))
  implementation(project(":nessie-versioned-transfer"))
  implementation(project(":nessie-versioned-transfer-proto"))
  implementation(project(":nessie-model"))
  implementation(project(":nessie-notice"))

  implementation(project(":nessie-catalog-service-transfer"))

  implementation(project(":nessie-versioned-storage-store"))
  implementation(project(":nessie-versioned-storage-bigtable"))
  implementation(project(":nessie-versioned-storage-cache"))
  implementation(project(":nessie-versioned-storage-cassandra"))
  implementation(project(":nessie-versioned-storage-cassandra2"))
  implementation(project(":nessie-versioned-storage-cleanup"))
  implementation(project(":nessie-versioned-storage-common"))
  implementation(project(":nessie-versioned-storage-dynamodb"))
  implementation(project(":nessie-versioned-storage-dynamodb2"))
  implementation(project(":nessie-versioned-storage-inmemory"))
  implementation(project(":nessie-versioned-storage-jdbc"))
  implementation(project(":nessie-versioned-storage-jdbc2"))
  implementation(project(":nessie-versioned-storage-mongodb"))
  implementation(project(":nessie-versioned-storage-mongodb2"))
  implementation(project(":nessie-versioned-storage-rocksdb"))

  implementation(project(":nessie-catalog-service-common"))
  implementation(project(":nessie-tasks-api"))

  implementation(libs.guava)

  implementation(quarkusPlatform(project))
  implementation(quarkusExtension(project, "amazon-services"))
  implementation("io.quarkus:quarkus-picocli")

  implementation(platform(libs.jackson.bom))
  implementation("com.fasterxml.jackson.core:jackson-databind")
  implementation("com.fasterxml.jackson.core:jackson-annotations")

  implementation(libs.agroal.pool)
  implementation(libs.h2)
  implementation(libs.postgresql)

  compileOnly(libs.jakarta.annotation.api)
  compileOnly(libs.microprofile.openapi)
  compileOnly("com.fasterxml.jackson.core:jackson-annotations")

  compileOnly(project(":nessie-immutables-std"))
  annotationProcessor(project(":nessie-immutables-std", configuration = "processor"))

  testFixturesApi(project(":nessie-quarkus-tests"))
  testFixturesApi(project(":nessie-versioned-tests"))
  intTestImplementation(enforcedPlatform(libs.testcontainers.bom))
  intTestImplementation(project(":nessie-versioned-storage-mongodb-tests"))
  intTestImplementation(project(":nessie-versioned-storage-mongodb2-tests"))
  intTestImplementation(project(":nessie-versioned-storage-jdbc-tests"))
  intTestImplementation(project(":nessie-versioned-storage-jdbc2-tests"))
  intTestImplementation(project(":nessie-versioned-storage-cassandra-tests"))
  intTestImplementation(project(":nessie-versioned-storage-cassandra2-tests"))
  intTestImplementation(project(":nessie-versioned-storage-bigtable-tests"))
  intTestImplementation(project(":nessie-versioned-storage-dynamodb-tests"))
  intTestImplementation(project(":nessie-versioned-storage-dynamodb2-tests"))
  intTestImplementation(project(":nessie-multi-env-test-engine"))
  testFixturesApi(project(":nessie-versioned-storage-testextension"))
  testFixturesApi(quarkusPlatform(project))
  testFixturesApi("io.quarkus:quarkus-junit5")
  testFixturesApi(libs.microprofile.openapi)

  testFixturesApi(platform(libs.junit.bom))
  testFixturesApi(libs.bundles.junit.testing)
}

tasks.withType<ProcessResources>().configureEach {
  from("src/main/resources") {
    expand("nessieVersion" to version)
    duplicatesStrategy = DuplicatesStrategy.INCLUDE
  }
}

val packageType = quarkusPackageType()

quarkus {
  quarkusBuildProperties.put("quarkus.package.type", packageType)
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

if (quarkusFatJar()) {
  afterEvaluate {
    publishing {
      publications {
        named<MavenPublication>("maven") {
          val quarkusBuild = tasks.getByName<QuarkusBuild>("quarkusBuild")
          artifact(quarkusBuild.runnerJar) {
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

tasks.named<Test>("intTest").configure {
  // Reduce likelihood of OOM due to too many Quarkus apps in memory;
  // Ideally, set this number to the number of IT classes to run for each backend.
  forkEvery = 6
  // Optional; comma-separated list of backend names to test against;
  // see NessieServerAdminTestBackends for valid values.
  systemProperty("backends", System.getProperty("backends"))
}

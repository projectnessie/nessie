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

plugins { id("nessie-conventions-java21") }

publishingHelper { mavenName = "Nessie - Quarkus Common" }

cassandraDriverTweak()

dependencies {
  implementation(project(":nessie-model"))
  implementation(project(":nessie-rest-common"))
  implementation(project(":nessie-server-store"))
  implementation(project(":nessie-services"))
  implementation(project(":nessie-services-config"))
  implementation(project(":nessie-versioned-spi"))
  implementation(project(":nessie-catalog-files-api"))
  implementation(project(":nessie-catalog-files-impl"))
  implementation(project(":nessie-catalog-service-common"))
  implementation(project(":nessie-catalog-secrets-api"))
  implementation(project(":nessie-quarkus-config"))

  compileOnly(project(":nessie-doc-generator-annotations"))

  implementation(project(":nessie-versioned-storage-bigtable"))
  implementation(project(":nessie-versioned-storage-cache"))
  implementation(project(":nessie-versioned-storage-cassandra"))
  implementation(project(":nessie-versioned-storage-cassandra2"))
  implementation(project(":nessie-versioned-storage-common"))
  implementation(project(":nessie-versioned-storage-dynamodb"))
  implementation(project(":nessie-versioned-storage-dynamodb2"))
  implementation(project(":nessie-versioned-storage-inmemory"))
  implementation(project(":nessie-versioned-storage-jdbc"))
  implementation(project(":nessie-versioned-storage-jdbc2"))
  implementation(project(":nessie-versioned-storage-mongodb"))
  implementation(project(":nessie-versioned-storage-mongodb2"))
  implementation(project(":nessie-versioned-storage-rocksdb"))
  implementation(project(":nessie-versioned-storage-store"))

  implementation(quarkusPlatform(project))
  implementation("io.quarkus:quarkus-mongodb-client")
  implementation("io.quarkus:quarkus-hibernate-validator")
  implementation("io.quarkus:quarkus-agroal")
  implementation("io.quarkus:quarkus-jdbc-postgresql")
  implementation("io.quarkus:quarkus-jdbc-mariadb")
  implementation("io.quarkus:quarkus-jdbc-h2")
  implementation("io.quarkus:quarkus-opentelemetry")
  implementation("io.quarkus:quarkus-micrometer")
  implementation("io.smallrye.config:smallrye-config-source-keystore")
  implementation(quarkusExtension(project, "amazon-services"))
  implementation("io.quarkiverse.amazonservices:quarkus-amazon-dynamodb")
  implementation("software.amazon.awssdk:sts")
  implementation("software.amazon.awssdk:apache-client") {
    exclude("commons-logging", "commons-logging")
  }
  implementation(quarkusExtension(project, "google-cloud-services"))
  implementation("io.quarkiverse.googlecloudservices:quarkus-google-cloud-bigtable")
  implementation(quarkusExtension(project, "cassandra"))
  implementation(enforcedPlatform(libs.cassandra.driver.bom))
  implementation("com.datastax.oss.quarkus:cassandra-quarkus-client") {
    // spotbugs-annotations has only a GPL license!
    exclude("com.github.spotbugs", "spotbugs-annotations")
  }

  implementation("com.fasterxml.jackson.core:jackson-databind")
  compileOnly("com.fasterxml.jackson.core:jackson-annotations")

  compileOnly(project(":nessie-immutables-std"))
  annotationProcessor(project(":nessie-immutables-std", configuration = "processor"))

  implementation("org.jboss.slf4j:slf4j-jboss-logmanager")
  implementation("io.opentelemetry:opentelemetry-opencensus-shim") // for Google BigTable
  implementation("io.micrometer:micrometer-core")

  implementation(libs.guava)

  compileOnly(libs.jakarta.validation.api)

  compileOnly(platform(libs.jackson.bom))
  compileOnly("com.fasterxml.jackson.core:jackson-annotations")

  compileOnly(libs.microprofile.openapi)

  testFixturesApi(platform(libs.junit.bom))
  testFixturesApi(libs.bundles.junit.testing)

  testFixturesApi(quarkusPlatform(project))
  testFixturesApi("io.quarkus:quarkus-core")

  testRuntimeOnly(libs.logback.classic)
}

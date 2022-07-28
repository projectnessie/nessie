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

plugins {
  `java-platform`
  `maven-publish`
  signing
  id("org.projectnessie.buildsupport.ide-integration")
  `nessie-conventions`
  id("io.github.gradle-nexus.publish-plugin")
}

extra["maven.name"] = "Nessie"

description = "Transactional Catalog for Data Lakes"

/*
Main dependency handling happens in this build script.
Declare versions as variables and add dependency constraints.
This works for GitHub's dependabot, it can scan this file and create version-bump PRs.

To use dependencies (without versions) from a "bom", import the bom using e.g.
    implementation(platform("bom-coordinates"))
    implementation("dependency-in-bom")

The following declarations also contain some build dependencies.
*/

val versionAgroalPool = "2.0"
val versionAgrona = "1.16.0"
val versionAntlr = "4.10.1"
val versionAssertJ = "3.23.1"
val versionAwssdk = "2.17.239"
val versionBouncyCastle = "1.70"
val versionCel = "0.3.8"
val versionCheckstyle = "10.3.1"
// to fix circular dependencies with NessieClient, certain projects need to use the same Nessie
// version as Iceberg/Delta has
var versionClientNessie = "0.30.0"
val versionDeltalake = "1.1.0-nessie"
val versionDockerjava = "3.2.13"
val versionErrorProneAnnotations = "2.14.0"
val versionErrorProneCore = "2.14.0"
val versionErrorProneSlf4j = "0.1.12"
val versionGatling = "3.8.2"
val versionGuava = "31.1-jre"
val versionGoogleJavaFormat = "1.15.0"
val versionGraalSvm = "22.2.0"
val versionH2 = "2.1.214"
val versionHadoop = "3.3.3"
var versionIceberg = "0.14.0"
val versionImmutables = "2.9.0"
val versionJackson = "2.13.3"
val versionJacksonSpark3 = "2.13.3"
val versionJacoco = "0.8.8"
val versionJakartaAnnotationApi = "1.3.5"
val versionJakartaEnterpriseCdiApi = "2.0.2"
val versionJakartaValidationApi = "2.0.2"
val versionJandex = "2.4.3.Final"
val versionJavaxServlet = "4.0.1"
val versionJavaxWsRs = "2.1.1"
val versionJaxrsApi21Spec = "2.0.2.Final"
val versionJsr305 = "3.0.2"
val versionJersey = "2.35"
val versionJmh = "1.35"
val versionJunit = "5.8.2"
val versionLogback = "1.2.11"
val versionMavenResolver = "1.7.3"
val versionMaven = "3.8.6"
val versionMockito = "4.6.1"
val versionMongodbDriverSync = "4.7.0"
val versionNessieApprunner = "0.21.4"
val versionOpenapi = "3.0"
val versionQuarkus = dependencyVersion("versionQuarkus") // ensure that plugin version is the same
val versionQuarkusAmazon = "2.10.3.Final"
val versionQuarkusLoggingSentry = "1.2.1"
val versionParquet = "1.12.3"
val versionPicocli = "4.6.3"
val versionPostgres = "42.4.0"
val versionProtobuf = "3.21.4"
val versionReactor = "2020.0.21"
val versionRestAssured = "5.1.1"
val versionRocksDb = "7.4.3"
val versionSlf4j = "1.7.36"
val versionTestcontainers = "1.17.3"
val versionWeld = "3.1.8.Final"

// Allow overriding the Iceberg version used by Nessie
versionIceberg = System.getProperty("nessie.versionIceberg", versionIceberg)

// Allow overriding the Nessie version used by integration tests that depend on Iceberg
versionClientNessie = System.getProperty("nessie.versionClientNessie", versionClientNessie)

extra["versionAntlr"] = versionAntlr

extra["versionCheckstyle"] = versionCheckstyle

extra["versionClientNessie"] = versionClientNessie

extra["versionErrorProneAnnotations"] = versionErrorProneAnnotations

extra["versionErrorProneCore"] = versionErrorProneCore

extra["versionErrorProneSlf4j"] = versionErrorProneSlf4j

extra["versionGatling"] = versionGatling

extra["versionGoogleJavaFormat"] = versionGoogleJavaFormat

extra["versionJacoco"] = versionJacoco

extra["versionJandex"] = versionJandex

extra["versionProtobuf"] = versionProtobuf

extra["versionRocksDb"] = versionRocksDb

extra["quarkus.builder-image"] = "quay.io/quarkus/ubi-quarkus-native-image:22.1-java17"

for (e in loadProperties(file("clients/spark-scala.properties"))) {
  extra[e.key.toString()] = e.value
}

// Dummy configuration to allow dependabot to manage dependencies here, but not include those
// dependencies in the constraints. Those "managedOnly" dependencies do not "leak" to other
// projects.
configurations.create("managedOnly")

dependencies {
  add("managedOnly", "org.antlr:antlr4:$versionAntlr")
  add("managedOnly", "org.antlr:antlr4-runtime:$versionAntlr")

  constraints {
    api("ch.qos.logback:logback-access:$versionLogback")
    api("ch.qos.logback:logback-classic:$versionLogback")
    api("ch.qos.logback:logback-core:$versionLogback")
    api("com.fasterxml.jackson:jackson-bom:$versionJackson")
    api("com.fasterxml.jackson.core:jackson-databind:$versionJacksonSpark3")
    api("com.fasterxml.jackson.module:jackson-module-scala_2.12:$versionJacksonSpark3")
    api("com.github.docker-java:docker-java-api:$versionDockerjava")
    api("com.google.code.findbugs:jsr305:$versionJsr305")
    api("com.google.errorprone:error_prone_annotations:$versionErrorProneAnnotations")
    api("com.google.errorprone:error_prone_core:$versionErrorProneCore")
    api("com.google.googlejavaformat:google-java-format:$versionGoogleJavaFormat")
    api("com.google.guava:guava:$versionGuava")
    api("com.google.protobuf:protobuf-java:$versionProtobuf")
    api("com.h2database:h2:$versionH2")
    api("com.puppycrawl.tools:checkstyle:$versionCheckstyle")
    api("info.picocli:picocli:$versionPicocli")
    api("info.picocli:picocli-codegen:$versionPicocli")
    api("io.agroal:agroal-pool:$versionAgroalPool")
    api("io.delta:delta-core_2.12:$versionDeltalake")
    api("io.gatling.highcharts:gatling-charts-highcharts:$versionGatling")
    api("io.projectreactor:reactor-bom:$versionReactor")
    api("io.quarkus.platform:quarkus-amazon-services-bom:$versionQuarkusAmazon")
    api("io.quarkus:quarkus-bom:$versionQuarkus")
    api("io.quarkiverse.loggingsentry:quarkus-logging-sentry:$versionQuarkusLoggingSentry")
    api("io.rest-assured:rest-assured:$versionRestAssured")
    api("jakarta.validation:jakarta.annotation-api:$versionJakartaAnnotationApi")
    api("jakarta.enterprise:jakarta.enterprise.cdi-api:$versionJakartaEnterpriseCdiApi")
    api("jakarta.validation:jakarta.validation-api:$versionJakartaValidationApi")
    api("javax.servlet:javax.servlet-api:$versionJavaxServlet")
    api("javax.ws.rs:javax.ws.rs-api:$versionJavaxWsRs")
    api("jp.skypencil.errorprone.slf4j:errorprone-slf4j:$versionErrorProneSlf4j")
    api("org.agrona:agrona:$versionAgrona")
    api("org.assertj:assertj-core:$versionAssertJ")
    api("org.apache.hadoop:hadoop-client:$versionHadoop")
    api("org.apache.iceberg:iceberg-api:$versionIceberg")
    api("org.apache.iceberg:iceberg-bundled-guava:$versionIceberg")
    api("org.apache.iceberg:iceberg-common:$versionIceberg")
    api("org.apache.iceberg:iceberg-core:$versionIceberg")
    api("org.apache.iceberg:iceberg-hive-metastore:$versionIceberg")
    api("org.apache.iceberg:iceberg-nessie:$versionIceberg")
    api("org.apache.iceberg:iceberg-parquet:$versionIceberg")
    for (sparkVersion in project.extra["sparkVersions"].toString().split(",")) {
      for (scalaVersion in
        project.extra["sparkVersion-$sparkVersion-scalaVersions"].toString().split(",")) {
        api("org.apache.iceberg:iceberg-spark-${sparkVersion}_$scalaVersion:$versionIceberg")
        api(
          "org.apache.iceberg:iceberg-spark-extensions-${sparkVersion}_$scalaVersion:$versionIceberg"
        )
      }
    }
    api("org.apache.maven:maven-resolver-provider:$versionMaven")
    api("org.apache.maven.resolver:maven-resolver-connector-basic:$versionMavenResolver")
    api("org.apache.maven.resolver:maven-resolver-transport-file:$versionMavenResolver")
    api("org.apache.maven.resolver:maven-resolver-transport-http:$versionMavenResolver")
    api("org.apache.parquet:parquet-column:$versionParquet")
    api("org.bouncycastle:bcprov-jdk15on:$versionBouncyCastle")
    api("org.bouncycastle:bcpkix-jdk15on:$versionBouncyCastle")
    api("org.eclipse.microprofile.openapi:microprofile-openapi-api:$versionOpenapi")
    api("org.glassfish.jersey:jersey-bom:$versionJersey")
    api("org.graalvm.nativeimage:svm:$versionGraalSvm")
    api("org.immutables:builder:$versionImmutables")
    api("org.immutables:value-annotations:$versionImmutables")
    api("org.immutables:value-fixture:$versionImmutables")
    api("org.immutables:value-processor:$versionImmutables")
    api("org.jacoco:jacoco-maven-plugin:$versionJacoco")
    api("org.jboss:jandex:$versionJandex")
    api("org.jboss.weld.se:weld-se-core:$versionWeld")
    api("org.jboss.spec.javax.ws.rs:jboss-jaxrs-api_2.1_spec:$versionJaxrsApi21Spec")
    api("org.junit:junit-bom:$versionJunit")
    api("org.mongodb:mongodb-driver-sync:$versionMongodbDriverSync")
    api("org.openjdk.jmh:jmh-core:$versionJmh")
    api("org.openjdk.jmh:jmh-generator-annprocess:$versionJmh")
    api("org.postgresql:postgresql:$versionPostgres")
    api("org.mockito:mockito-core:$versionMockito")
    api("org.projectnessie.cel:cel-bom:$versionCel")
    api("org.rocksdb:rocksdbjni:$versionRocksDb")
    api("org.slf4j:jcl-over-slf4j:$versionSlf4j")
    api("org.slf4j:log4j-over-slf4j:$versionSlf4j")
    api("org.slf4j:slf4j-api:$versionSlf4j")
    api("org.testcontainers:cockroachdb:$versionTestcontainers")
    api("org.testcontainers:mongodb:$versionTestcontainers")
    api("org.testcontainers:postgresql:$versionTestcontainers")
    api("org.testcontainers:testcontainers:$versionTestcontainers")
    api("software.amazon.awssdk:bom:$versionAwssdk")
  }
}

javaPlatform { allowDependencies() }

tasks.named<Wrapper>("wrapper") { distributionType = Wrapper.DistributionType.ALL }

// Pass environment variables:
//    ORG_GRADLE_PROJECT_sonatypeUsername
//    ORG_GRADLE_PROJECT_sonatypePassword
// OR in ~/.gradle/gradle.properties set
//    sonatypeUsername
//    sonatypePassword
// Call targets:
//    publishToSonatype
//    closeAndReleaseSonatypeStagingRepository
nexusPublishing {
  transitionCheckOptions {
    // default==60 (10 minutes), wait up to 60 minutes
    maxRetries.set(360)
    // default 10s
    delayBetween.set(java.time.Duration.ofSeconds(10))
  }
  repositories { sonatype() }
}

val buildToolIntegrationGradle by
  tasks.registering(Exec::class) {
    group = "Verification"
    description =
      "Checks whether the bom works fine with Gradle, requires preceding publishToMavenLocal in a separate Gradle invocation"

    workingDir = file("build-tools-integration-tests")
    commandLine("${project.projectDir}/gradlew", "-p", workingDir, "test")
  }

val buildToolIntegrationMaven by
  tasks.registering(Exec::class) {
    group = "Verification"
    description =
      "Checks whether the bom works fine with Maven, requires preceding publishToMavenLocal in a separate Gradle invocation"

    workingDir = file("build-tools-integration-tests")
    commandLine("./mvnw", "--batch-mode", "clean", "test", "-Dnessie.version=${project.version}")
  }

val buildToolsIntegrationTest by
  tasks.registering {
    group = "Verification"
    description =
      "Checks whether the bom works fine with build tools, requires preceding publishToMavenLocal in a separate Gradle invocation"

    dependsOn(buildToolIntegrationGradle)
    dependsOn(buildToolIntegrationMaven)
  }

publishingHelper {
  nessieRepoName.set("nessie")
  inceptionYear.set("2020")
}

spotless {
  kotlinGradle {
    // Must be repeated :( - there's no "addTarget" or so
    target("nessie-iceberg/*.gradle.kts", "*.gradle.kts", "buildSrc/*.gradle.kts")
  }
}

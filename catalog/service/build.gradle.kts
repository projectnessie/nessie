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
  id("nessie-conventions-server")
  id("nessie-jacoco")
}

description = "Nessie - REST Catalog - Service"

extra["maven.name"] = "Nessie - REST Catalog - Service"

val versionIceberg = libs.versions.iceberg.get()

val sparkScala = useSparkScalaVersionsForProject("3.4", "2.12")

dependencies {
  implementation(nessieProject(":nessie-client"))

  implementation("org.apache.iceberg:iceberg-core:$versionIceberg")
  implementation(nessieProject(":nessie-catalog-api"))

  implementation(libs.hadoop.common) { hadoopExcludes() }

  compileOnly(libs.immutables.builder)
  compileOnly(libs.immutables.value.annotations)
  annotationProcessor(libs.immutables.value.processor)

  implementation(libs.guava)
  implementation(libs.agrona)

  implementation(platform(libs.jackson.bom))
  implementation("com.fasterxml.jackson.core:jackson-databind")
  implementation("com.fasterxml.jackson.core:jackson-annotations")

  // javax/jakarta
  compileOnly(libs.jakarta.ws.rs.api)
  compileOnly(libs.javax.ws.rs)
  compileOnly(libs.jakarta.enterprise.cdi.api)
  compileOnly(libs.javax.enterprise.cdi.api)
  compileOnly(libs.jakarta.validation.api)
  compileOnly(libs.javax.validation.api)
  compileOnly(libs.findbugs.jsr305)

  compileOnly(libs.microprofile.openapi)

  testFixturesApi(nessieProject(":nessie-client"))
  testFixturesApi(libs.guava)

  testFixturesApi("org.apache.iceberg:iceberg-core:$versionIceberg")
  testFixturesApi(nessieProject(":nessie-catalog-api"))
  testFixturesApi(libs.hadoop.common) { hadoopExcludes() }

  testFixturesCompileOnly(libs.immutables.builder)
  testFixturesCompileOnly(libs.immutables.value.annotations)
  testFixturesAnnotationProcessor(libs.immutables.value.processor)

  // javax/jakarta
  testFixturesImplementation(libs.jakarta.enterprise.cdi.api)
  testFixturesImplementation(libs.javax.enterprise.cdi.api)

  testFixturesImplementation(libs.microprofile.openapi)

  testFixturesImplementation(platform(libs.jersey.bom))
  testFixturesImplementation("org.glassfish.jersey.core:jersey-server")
  testFixturesImplementation("org.glassfish.jersey.inject:jersey-hk2")
  testFixturesImplementation("org.glassfish.jersey.media:jersey-media-json-jackson")
  testFixturesImplementation("org.glassfish.jersey.ext:jersey-bean-validation")
  testFixturesImplementation("org.glassfish.jersey.ext.cdi:jersey-cdi1x")
  testFixturesImplementation("org.glassfish.jersey.ext.cdi:jersey-cdi-rs-inject")
  testFixturesImplementation("org.glassfish.jersey.ext.cdi:jersey-weld2-se")

  testFixturesImplementation(
    platform("org.glassfish.jersey.test-framework.providers:jersey-test-framework-provider-bundle")
  )
  testFixturesImplementation("org.glassfish.jersey.test-framework:jersey-test-framework-core")
  testFixturesImplementation("org.glassfish.jersey.test-framework:jersey-test-framework-util")
  testFixturesImplementation(
    "org.glassfish.jersey.test-framework.providers:jersey-test-framework-provider-grizzly2"
  )
  testFixturesImplementation(
    "org.glassfish.jersey.test-framework.providers:jersey-test-framework-provider-inmemory"
  )
  testFixturesImplementation(
    "org.glassfish.jersey.test-framework.providers:jersey-test-framework-provider-external"
  )
  testFixturesImplementation(
    "org.glassfish.jersey.test-framework.providers:jersey-test-framework-provider-jdk-http"
  )
  testFixturesImplementation(
    "org.glassfish.jersey.test-framework.providers:jersey-test-framework-provider-simple"
  )
  testFixturesImplementation(
    "org.glassfish.jersey.test-framework.providers:jersey-test-framework-provider-jetty"
  )
  testFixturesImplementation("org.jboss.weld.se:weld-se-core")

  testFixturesApi(platform(libs.junit.bom))
  testFixturesApi(libs.bundles.junit.testing)

  testFixturesApi("org.apache.iceberg:iceberg-api:$versionIceberg:tests")
  testFixturesApi("org.apache.iceberg:iceberg-core:$versionIceberg:tests")
  testFixturesImplementation(libs.junit4)

  testFixturesImplementation(libs.slf4j.api)
  testFixturesRuntimeOnly(libs.logback.classic)

  testFixturesApi(nessieProject(":nessie-jaxrs-testextension"))
  testFixturesApi(nessieProject(":nessie-versioned-storage-inmemory"))

  testCompileOnly(libs.microprofile.openapi)

  intTestImplementation(
    nessieProject(
      ":nessie-spark-extensions-${sparkScala.sparkMajorVersion}_${sparkScala.scalaMajorVersion}"
    )
  )
  intTestImplementation("org.apache.iceberg:iceberg-nessie:$versionIceberg")
  intTestImplementation(
    "org.apache.iceberg:iceberg-spark-${sparkScala.sparkMajorVersion}_${sparkScala.scalaMajorVersion}:$versionIceberg"
  )
  intTestImplementation(
    "org.apache.iceberg:iceberg-spark-extensions-${sparkScala.sparkMajorVersion}_${sparkScala.scalaMajorVersion}:$versionIceberg"
  )
  intTestImplementation("org.apache.iceberg:iceberg-hive-metastore:$versionIceberg")

  intTestRuntimeOnly(libs.logback.classic)
  intTestImplementation(libs.slf4j.log4j.over.slf4j)
  intTestImplementation("org.apache.spark:spark-sql_${sparkScala.scalaMajorVersion}") {
    forSpark(sparkScala.sparkVersion)
  }
  intTestImplementation("org.apache.spark:spark-core_${sparkScala.scalaMajorVersion}") {
    forSpark(sparkScala.sparkVersion)
  }
  intTestImplementation("org.apache.spark:spark-hive_${sparkScala.scalaMajorVersion}") {
    forSpark(sparkScala.sparkVersion)
  }
  intTestImplementation(
    nessieProject(":nessie-spark-extensions-basetests_${sparkScala.scalaMajorVersion}")
  )
}

fun ModuleDependency.hadoopExcludes() {
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

forceJava11ForTestTask("intTest")

/*
 * Copyright (C) 2023 Dremio
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

import org.apache.tools.ant.taskdefs.condition.Os

plugins {
  id("nessie-conventions-server")
  id("nessie-jacoco")
}

description = "Nessie - REST Catalog - Spark/Iceberg integration tests"

val versionIceberg = libs.versions.iceberg.get()

val sparkScala = useSparkScalaVersionsForProject("3.3", "2.12")

configurations.register("nessieCatalogServer")

dependencies {
  testFixturesApi("org.apache.iceberg:iceberg-core:$versionIceberg")
  testFixturesApi(libs.hadoop.common) { hadoopExcludes() }

  testFixturesImplementation(libs.microprofile.openapi)

  testFixturesApi(platform(libs.junit.bom))
  testFixturesApi(libs.bundles.junit.testing)

  testFixturesApi("org.apache.iceberg:iceberg-api:$versionIceberg:tests")
  testFixturesApi("org.apache.iceberg:iceberg-core:$versionIceberg:tests")
  testFixturesImplementation(libs.junit4)

  testFixturesImplementation(libs.slf4j.api)

  testCompileOnly(libs.microprofile.openapi)

  intTestRuntimeOnly(libs.logback.classic)
  intTestImplementation(libs.slf4j.log4j.over.slf4j)

  intTestImplementation(nessieProject("nessie-client"))
  intTestImplementation(nessieProject("nessie-keycloak-testcontainer"))
  intTestImplementation(libs.keycloak.admin.client)
  intTestImplementation(nessieProject("nessie-nessie-testcontainer"))
  // Keycloak-admin-client depends on Resteasy.
  // Need to bump Resteasy, because Resteasy < 6.2.4 clashes with our Jackson version management and
  // cause non-existing jackson versions like 2.15.2-jakarta, which then lets the build fail.
  intTestImplementation(platform(libs.resteasy.bom))

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

  intTestImplementation("org.projectnessie.nessie-runner:nessie-runner-common:0.30.6")

  add("nessieCatalogServer", nessieProject(":nessie-catalog-server", "quarkusRunner"))
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

forceJavaVersionForTestTask("intTest", sparkScala.runtimeJavaVersion)

tasks.withType<Test>().configureEach {
  systemProperty("keycloak.docker.tag", libs.versions.keycloak.get())

  dependsOn(configurations.named("nessieCatalogServer"))
  jvmArgumentProviders.add(
    CommandLineArgumentProvider {
      val javaExec = javaLauncher.get().executablePath.asFile.absolutePath
      val catalogServerJar =
        configurations
          .named("nessieCatalogServer")
          .get()
          .resolvedConfiguration
          .files
          .map { f -> f.absolutePath }
          .joinToString(";")

      listOf("-Dnessie-catalog-server-jar=$catalogServerJar", "-DjavaExec=$javaExec")
    }
  )
}

// Testcontainers is not supported on Windows :(
if (Os.isFamily(Os.FAMILY_WINDOWS)) {
  tasks.withType<Test>().configureEach { this.enabled = false }
}

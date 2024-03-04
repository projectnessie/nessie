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
  id("nessie-conventions-client")
  id("nessie-jacoco")
  alias(libs.plugins.annotations.stripper)
}

extra["maven.name"] = "Nessie - Client"

dependencies {
  api(project(":nessie-model"))

  implementation(platform(libs.jackson.bom))
  implementation("com.fasterxml.jackson.core:jackson-core")
  implementation("com.fasterxml.jackson.core:jackson-databind")
  implementation("com.fasterxml.jackson.core:jackson-annotations")
  implementation(libs.microprofile.openapi)

  // javax/jakarta
  compileOnly(libs.jakarta.annotation.api)
  compileOnly(libs.findbugs.jsr305)
  compileOnly(libs.jakarta.validation.api)
  compileOnly(libs.javax.validation.api)
  compileOnly(libs.jakarta.ws.rs.api)
  compileOnly(libs.javax.ws.rs)

  implementation(libs.slf4j.api)
  compileOnly(libs.errorprone.annotations)

  compileOnly(libs.immutables.builder)
  compileOnly(libs.immutables.value.annotations)
  annotationProcessor(libs.immutables.value.processor)

  testFixturesApi(libs.guava)
  testFixturesApi(libs.bouncycastle.bcprov)
  testFixturesApi(libs.bouncycastle.bcpkix)
  testFixturesApi(libs.mockito.core)

  testFixturesApi(platform(libs.junit.bom))
  testFixturesApi(libs.bundles.junit.testing)

  compileOnly(platform(libs.opentelemetry.bom.alpha))
  compileOnly("io.opentelemetry:opentelemetry-api")

  compileOnly(platform(libs.awssdk.bom))
  compileOnly("software.amazon.awssdk:auth")

  // javax/jakarta
  testFixturesApi(libs.jakarta.annotation.api)

  testFixturesApi(platform(libs.opentelemetry.bom.alpha))
  testFixturesApi("io.opentelemetry:opentelemetry-api")
  testFixturesApi("io.opentelemetry:opentelemetry-sdk")
  testFixturesApi("io.opentelemetry:opentelemetry-exporter-otlp")
  testFixturesApi(platform(libs.awssdk.bom))
  testFixturesApi("software.amazon.awssdk:auth")
  testFixturesApi(libs.undertow.core)
  testFixturesApi(libs.undertow.servlet)
  testFixturesImplementation(libs.logback.classic)

  testImplementation(libs.wiremock)

  intTestImplementation(platform(libs.testcontainers.bom))
  intTestImplementation("org.testcontainers:testcontainers")
  intTestImplementation("org.testcontainers:junit-jupiter")
  intTestImplementation(libs.keycloak.admin.client)
  intTestImplementation(libs.testcontainers.keycloak) {
    exclude(group = "org.slf4j") // uses SLF4J 2.x, we are not ready yet
  }
}

jandex { skipDefaultProcessing() }

val jacksonTestVersions =
  setOf(
    "2.12.3", // Spark 3.2.1+3.2.2
    "2.13.4", // Spark 3.3
    "2.14.2", // Spark 3.4
    "2.15.2", // Spark 3.5
  )

@Suppress("UnstableApiUsage")
fun JvmComponentDependencies.forJacksonVersion(jacksonVersion: String) {
  implementation(project())

  implementation("com.fasterxml.jackson.core:jackson-core") { version { strictly(jacksonVersion) } }
  implementation("com.fasterxml.jackson.core:jackson-annotations") {
    version { strictly(jacksonVersion) }
  }
  implementation("com.fasterxml.jackson.core:jackson-databind") {
    version { strictly(jacksonVersion) }
  }
}

@Suppress("UnstableApiUsage")
fun JvmTestSuite.commonCompatSuite() {
  useJUnitJupiter(libsRequiredVersion("junit"))

  sources { java.srcDirs(sourceSets.getByName("test").java.srcDirs) }

  targets { all { tasks.named("check").configure { dependsOn(testTask) } } }
}

@Suppress("UnstableApiUsage")
fun JvmTestSuite.useJava8() {
  targets {
    all {
      testTask.configure {
        val javaToolchains = project.extensions.findByType(JavaToolchainService::class.java)
        javaLauncher = javaToolchains!!.launcherFor { languageVersion = JavaLanguageVersion.of(8) }
      }
    }
  }
}

@Suppress("UnstableApiUsage")
testing {
  suites {
    register("testJava8", JvmTestSuite::class.java) {
      commonCompatSuite()

      dependencies { forJacksonVersion(libs.jackson.bom.get().version!!) }

      useJava8()
    }

    configurations.named("testJava8Implementation") {
      extendsFrom(configurations.getByName("testImplementation"))
    }

    jacksonTestVersions.forEach { jacksonVersion ->
      val safeName = jacksonVersion.replace("[.]".toRegex(), "_")
      register("testJackson_$safeName", JvmTestSuite::class.java) {
        commonCompatSuite()

        dependencies { forJacksonVersion(jacksonVersion) }
      }

      configurations.named("testJackson_${safeName}Implementation") {
        extendsFrom(configurations.getByName("testImplementation"))
      }

      register("testJackson_${safeName}_java8", JvmTestSuite::class.java) {
        commonCompatSuite()

        dependencies { forJacksonVersion(jacksonVersion) }

        useJava8()

        configurations.named("testJackson_${safeName}_java8Implementation") {
          extendsFrom(configurations.getByName("testImplementation"))
        }
      }
    }
  }
}

annotationStripper {
  registerDefault().configure {
    annotationsToDrop("^jakarta[.].+".toRegex())
    unmodifiedClassesForJavaVersion = 11
  }
}

// prevent duplicate checkstyle work
(jacksonTestVersions
    .map { jacksonVersion -> "Jackson_" + jacksonVersion.replace("[.]".toRegex(), "_") }
    .flatMap { n -> listOf(n, n + "_java8") } + listOf("Java8"))
  .forEach { v -> tasks.named("checkstyleTest$v").configure { enabled = false } }

// Issue w/ testcontainers/podman in GH workflows :(
if ((Os.isFamily(Os.FAMILY_MAC) || Os.isFamily(Os.FAMILY_WINDOWS)) && System.getenv("CI") != null) {
  tasks.named<Test>("intTest").configure { this.enabled = false }
}

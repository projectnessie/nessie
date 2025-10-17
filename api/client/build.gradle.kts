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

plugins { id("nessie-conventions-java11") }

publishingHelper { mavenName = "Nessie - Client" }

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

  compileOnly(libs.httpclient5)

  implementation(libs.slf4j.api) { version { require(libs.versions.slf4j.compat.get()) } }
  compileOnly(libs.errorprone.annotations)

  compileOnly(project(":nessie-doc-generator-annotations"))

  compileOnly(project(":nessie-immutables-std"))
  annotationProcessor(project(":nessie-immutables-std", configuration = "processor"))

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
  testFixturesApi(libs.findbugs.jsr305)

  testFixturesApi(platform(libs.opentelemetry.bom.alpha))
  testFixturesApi("io.opentelemetry:opentelemetry-api")
  testFixturesApi("io.opentelemetry:opentelemetry-sdk")
  testFixturesApi("io.opentelemetry:opentelemetry-exporter-otlp")
  testFixturesApi(platform(libs.awssdk.bom))
  testFixturesApi("software.amazon.awssdk:auth")
  testFixturesApi(libs.undertow.core)
  testFixturesApi(libs.undertow.servlet)
  testFixturesApi(libs.httpclient5)
  testFixturesImplementation(libs.logback.classic) {
    version { require(libs.versions.logback.compat.get()) }
    // Logback 1.3 brings Slf4j 2.0, which doesn't work with Spark up to 3.3
    exclude("org.slf4j", "slf4j-api")
  }

  testImplementation(libs.wiremock)

  testRuntimeOnly(libs.logback.classic)

  testCompileOnly(project(":nessie-immutables-std"))
  testAnnotationProcessor(project(":nessie-immutables-std", configuration = "processor"))

  intTestImplementation(platform(libs.testcontainers.bom))
  intTestImplementation("org.testcontainers:testcontainers")
  intTestImplementation("org.testcontainers:testcontainers-junit-jupiter")
  intTestImplementation(libs.keycloak.admin.client)
  intTestImplementation(libs.testcontainers.keycloak) {
    exclude(group = "org.slf4j") // uses SLF4J 2.x, we are not ready yet
  }
  intTestImplementation(project(":nessie-container-spec-helper"))
  intTestCompileOnly(project(":nessie-immutables-std"))
}

tasks.named("jandex") { enabled = false }

val jacksonTestVersions =
  setOf(
    "2.13.4", // Spark 3.3
    "2.14.2", // Spark 3.4
    "2.15.2", // Spark 3.5
    // there's been some change from 2.18.2->.3, see #10489 &
    // org.projectnessie.client.rest.ResponseCheckFilter.decodeErrorObject
    "2.18.2",
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
testing {
  suites {
    register("testNoApacheHttp", JvmTestSuite::class.java) {
      useJUnitJupiter(libsRequiredVersion("junit"))

      sources { java.srcDirs(sourceSets.getByName("testNoApacheHttp").java.srcDirs) }

      targets {
        all {
          testTask.configure { useJUnitPlatform { includeTags("NoApacheHttp") } }

          tasks.named("check").configure { dependsOn(testTask) }
        }
      }
    }

    configurations.named("testNoApacheHttpImplementation") {
      extendsFrom(configurations.getByName("testImplementation"))
      exclude(group = "org.apache.httpcomponents.client5")
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
      configurations.named("testJackson_${safeName}CompileOnly") {
        extendsFrom(configurations.getByName("testCompileOnly"))
      }
      configurations.named("testJackson_${safeName}AnnotationProcessor") {
        extendsFrom(configurations.getByName("testAnnotationProcessor"))
      }
    }
  }
}

// prevent duplicate checkstyle work
jacksonTestVersions
  .map { jacksonVersion -> "Jackson_" + jacksonVersion.replace("[.]".toRegex(), "_") }
  .forEach { v -> tasks.named("checkstyleTest$v").configure { enabled = false } }

// Issue w/ testcontainers/podman in GH workflows :(
if ((Os.isFamily(Os.FAMILY_MAC) || Os.isFamily(Os.FAMILY_WINDOWS)) && System.getenv("CI") != null) {
  tasks.named<Test>("intTest").configure { this.enabled = false }
}

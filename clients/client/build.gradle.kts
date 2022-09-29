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
  `java-library`
  jacoco
  `maven-publish`
  signing
  `nessie-conventions`
}

extra["maven.name"] = "Nessie - Client"

dependencies {
  api(project(":nessie-model"))

  implementation(platform(libs.jackson.bom))
  implementation(libs.jackson.core)
  implementation(libs.jackson.databind)
  implementation(libs.jackson.annotations)
  implementation(libs.microprofile.openapi)
  compileOnly(libs.jakarta.validation.api)
  implementation(libs.javax.ws.rs)
  implementation(libs.findbugs.jsr305)
  compileOnly(libs.errorprone.annotations)

  compileOnly(libs.immutables.builder)
  compileOnly(libs.immutables.value.annotations)
  annotationProcessor(libs.immutables.value.processor)

  testImplementation(libs.guava)
  testImplementation(libs.bouncycastle.bcprov)
  testImplementation(libs.bouncycastle.bcpkix)
  testImplementation(libs.mockito.core)

  testImplementation(platform(libs.junit.bom))
  testImplementation(libs.bundles.junit.testing)
  testRuntimeOnly(libs.junit.jupiter.engine)

  compileOnly(libs.opentracing.util)
  compileOnly(platform(libs.awssdk.bom))
  compileOnly(libs.awssdk.auth)

  testImplementation(libs.opentracing.util)
  testImplementation(libs.jaeger.core)
  testImplementation(platform(libs.awssdk.bom))
  testImplementation(libs.awssdk.auth)
}

jandex { skipDefaultProcessing() }

val jacksonTestVersions =
  mapOf(
    "2.10.0" to "Spark 3.1.2+3.1.3",
    "2.11.4" to "Spark 3.?.? (reason unknown)",
    "2.12.3" to "Spark 3.2.1+3.2.2",
    "2.13.3" to "Spark 3.3.0"
  )

val testJava8 by
  tasks.registering(Test::class) {
    description = "Runs tests using URLConnection client using Java 8."
    group = "verification"

    val test = tasks.named<Test>("test").get()

    testClassesDirs = test.testClassesDirs
    classpath = test.classpath

    val javaToolchains = project.extensions.findByType(JavaToolchainService::class.java)
    javaLauncher.set(
      javaToolchains!!.launcherFor { languageVersion.set(JavaLanguageVersion.of(8)) }
    )
  }

val jacksonTests by
  tasks.registering {
    description = "Runs tests against Jackson versions ${jacksonTestVersions.keys}."
    group = "verification"
  }

tasks.named("check") { dependsOn(jacksonTests, testJava8) }

jacksonTestVersions.forEach { (jacksonVersion, reason) ->
  val safeName = jacksonVersion.replace("[.]".toRegex(), "_")
  val sourceSetName = "jackson_${safeName}_test"

  val sourceSets: SourceSetContainer by project
  sourceSets.create(sourceSetName) {
    java.srcDir(sourceSets.test.get().java)
    compileClasspath += sourceSets.main.get().output
    runtimeClasspath += sourceSets.main.get().output
  }

  val implConfigName = "${sourceSetName}Implementation"
  val runtimeConfigName = "${sourceSetName}RuntimeOnly"
  configurations.named(implConfigName) {
    extendsFrom(configurations.implementation.get())
    extendsFrom(configurations.testImplementation.get())
  }
  configurations.named(runtimeConfigName) {
    extendsFrom(configurations.runtimeOnly.get())
    extendsFrom(configurations.testRuntimeOnly.get())
  }

  dependencies.add(implConfigName, "com.fasterxml.jackson.core:jackson-core:$jacksonVersion!!")
  dependencies.add(
    implConfigName,
    "com.fasterxml.jackson.core:jackson-annotations:$jacksonVersion!!"
  )
  dependencies.add(implConfigName, "com.fasterxml.jackson.core:jackson-databind:$jacksonVersion!!")

  val taskName = "testJackson_$safeName"
  val testTask =
    tasks.register<Test>(taskName) {
      description = "Runs tests using Jackson $jacksonVersion for $reason."
      group = "verification"

      dependsOn("test")

      testClassesDirs = sourceSets[sourceSetName].output.classesDirs
      classpath = sourceSets[sourceSetName].runtimeClasspath
    }

  val taskName8 = "testJackson_${safeName}_java8"
  val testTask8 =
    tasks.register<Test>(taskName8) {
      description = "Runs tests using Jackson $jacksonVersion for $reason using Java 8."
      group = "verification"

      dependsOn("test")

      testClassesDirs = sourceSets[sourceSetName].output.classesDirs
      classpath = sourceSets[sourceSetName].runtimeClasspath

      val javaToolchains = project.extensions.findByType(JavaToolchainService::class.java)
      javaLauncher.set(
        javaToolchains!!.launcherFor { languageVersion.set(JavaLanguageVersion.of(8)) }
      )
    }

  jacksonTests { dependsOn(testTask, testTask8) }
}

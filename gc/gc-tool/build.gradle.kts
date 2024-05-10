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

import com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar

plugins {
  id("com.github.johnrengelman.shadow")
  id("nessie-conventions-iceberg")
  id("nessie-jacoco")
  id("nessie-shadow-jar")
  id("nessie-license-report")
}

extra["maven.name"] = "Nessie - GC - Standalone command line tool"

dependencies {
  implementation(nessieProject("nessie-client"))
  implementation(nessieProject("nessie-gc-base"))
  implementation(nessieProject("nessie-gc-iceberg"))
  implementation(nessieProject("nessie-gc-iceberg-files"))
  implementation(nessieProject("nessie-gc-repository-jdbc"))

  compileOnly(libs.errorprone.annotations)
  compileOnly(libs.immutables.value.annotations)
  annotationProcessor(libs.immutables.value.processor)

  implementation(platform(libs.iceberg.bom))
  implementation("org.apache.iceberg:iceberg-core")
  runtimeOnly("org.apache.iceberg:iceberg-hive-metastore")
  runtimeOnly("org.apache.iceberg:iceberg-aws")
  runtimeOnly("org.apache.iceberg:iceberg-gcp")
  runtimeOnly("org.apache.iceberg:iceberg-azure")

  // hadoop-common brings Jackson in ancient versions, pulling in the Jackson BOM to avoid that
  implementation(platform(libs.jackson.bom))
  implementation(libs.hadoop.common) {
    exclude("javax.servlet.jsp", "jsp-api")
    exclude("javax.ws.rs", "javax.ws.rs-api")
    exclude("log4j", "log4j")
    exclude("org.slf4j", "slf4j-log4j12")
    exclude("org.slf4j", "slf4j-reload4j")
    exclude("com.sun.jersey")
    exclude("org.eclipse.jetty")
    exclude("org.apache.hadoop")
    exclude("org.apache.zookeeper")
  }
  // Bump the jabx-impl version 2.2.3-1 via hadoop-common to make it work with Java 17+
  implementation(libs.jaxb.impl)

  implementation(platform(libs.awssdk.bom))
  runtimeOnly("software.amazon.awssdk:s3")
  runtimeOnly("software.amazon.awssdk:url-connection-client")
  runtimeOnly("software.amazon.awssdk:sts")

  implementation(platform(libs.google.cloud.storage.bom))
  runtimeOnly("com.google.cloud:google-cloud-storage")
  runtimeOnly(libs.google.cloud.nio)
  runtimeOnly(libs.google.cloud.gcs.connector)

  implementation(platform(libs.azuresdk.bom))
  runtimeOnly("com.azure:azure-storage-file-datalake")
  runtimeOnly("com.azure:azure-identity")
  runtimeOnly(libs.hadoop.azure)

  implementation(libs.picocli)
  annotationProcessor(libs.picocli.codegen)

  implementation(libs.slf4j.api)
  runtimeOnly(libs.logback.classic)

  compileOnly(libs.microprofile.openapi)
  compileOnly("com.fasterxml.jackson.core:jackson-annotations")

  compileOnly(libs.jakarta.validation.api)
  compileOnly(libs.jakarta.annotation.api)

  runtimeOnly(libs.h2)
  runtimeOnly(libs.postgresql)

  testCompileOnly(platform(libs.jackson.bom))

  testImplementation(nessieProject("nessie-jaxrs-testextension"))
  testImplementation(nessieProject("nessie-versioned-storage-inmemory-tests"))

  testRuntimeOnly(libs.logback.classic)

  testCompileOnly(libs.immutables.value.annotations)
  testAnnotationProcessor(libs.immutables.value.processor)

  testCompileOnly("com.fasterxml.jackson.core:jackson-annotations")
  testCompileOnly(libs.microprofile.openapi)

  testImplementation(platform(libs.junit.bom))
  testImplementation(libs.bundles.junit.testing)
}

val noticeDir = project.layout.buildDirectory.dir("notice")

val includeNoticeFile by
  tasks.registering(Sync::class) {
    destinationDir = noticeDir.get().asFile
    from(rootProject.projectDir) {
      into("META-INF/resources")
      include("NOTICE")
      rename { "NOTICE.txt" }
    }
  }

sourceSets.named("main") { resources.srcDir(noticeDir) }

tasks.named("processResources") { dependsOn(includeNoticeFile) }

tasks.named("sourcesJar") { dependsOn(includeNoticeFile) }

tasks.named<Test>("test").configure { systemProperty("expectedNessieVersion", project.version) }

val mainClassName = "org.projectnessie.gc.tool.cli.CLI"

val generateAutoComplete by
  tasks.creating(JavaExec::class.java) {
    group = "build"
    description = "Generates the bash/zsh autocompletion scripts"

    val compileJava = tasks.named<JavaCompile>("compileJava")

    dependsOn(compileJava)

    val completionScriptsDir =
      project.layout.buildDirectory.dir("classes/java/main/META-INF/completion")

    doFirst { mkdir(completionScriptsDir) }

    mainClass = "picocli.AutoComplete"
    classpath(configurations.named("runtimeClasspath"), compileJava)
    args(
      "--force",
      "-o",
      completionScriptsDir.get().dir("nessie-gc-completion").toString(),
      mainClassName
    )

    inputs.files("src/main").withPathSensitivity(PathSensitivity.RELATIVE)
    outputs.dir(completionScriptsDir)
  }

// generateAutoComplete writes the bash/zsh completion script into the main resource output,
// which is a bit ugly, but works. But the following tasks need a dependency to that task so that
// Gradle can properly evaluate the dependencies.
listOf("compileTestJava", "jandexMain", "jar", "shadowJar").forEach { t ->
  tasks.named(t).configure { dependsOn(generateAutoComplete) }
}

val shadowJar = tasks.named<ShadowJar>("shadowJar")

val copyUberJar by tasks.registering(Copy::class)

copyUberJar.configure {
  group = "build"
  description = "Copies the uber-jar to build/executable"
  dependsOn(shadowJar)
  from(shadowJar.get().archiveFile)
  into(project.layout.buildDirectory.dir("executable"))
  rename { "nessie-gc.jar" }
}

shadowJar.configure {
  manifest { attributes["Main-Class"] = mainClassName }
  finalizedBy(copyUberJar)
}

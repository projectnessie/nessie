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
  `java-library`
  jacoco
  `maven-publish`
  signing
  id("com.github.johnrengelman.shadow")
  `nessie-conventions`
}

extra["maven.name"] = "Nessie - GC - Standalone command line tool"

dependencies {
  implementation(platform(nessieRootProject()))
  implementation(nessieProjectPlatform("nessie-deps-iceberg", gradle))
  compileOnly(nessieProjectPlatform("nessie-deps-build-only", gradle))
  annotationProcessor(nessieProjectPlatform("nessie-deps-build-only", gradle))
  compileOnly(platform("com.fasterxml.jackson:jackson-bom"))
  implementation(platform("software.amazon.awssdk:bom:${dependencyVersion("versionAwssdk")}"))

  compileOnly("com.google.errorprone:error_prone_annotations")
  compileOnly("org.immutables:value-annotations")
  annotationProcessor("org.immutables:value-processor")

  implementation(nessieProject("nessie-client"))
  implementation(nessieProject("nessie-gc-base"))
  implementation(nessieProject("nessie-gc-iceberg"))
  implementation(nessieProject("nessie-gc-iceberg-files"))
  implementation(nessieProject("nessie-gc-repository-jdbc"))

  implementation("org.apache.iceberg:iceberg-core")
  runtimeOnly("org.apache.iceberg:iceberg-hive-metastore")
  runtimeOnly("org.apache.iceberg:iceberg-aws")

  implementation("org.apache.hadoop:hadoop-common") {
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

  runtimeOnly("software.amazon.awssdk:s3")
  runtimeOnly("software.amazon.awssdk:url-connection-client")

  implementation("info.picocli:picocli")
  annotationProcessor("info.picocli:picocli-codegen")

  implementation("org.slf4j:slf4j-api")
  runtimeOnly("ch.qos.logback:logback-classic:${dependencyVersion("versionLogback")}")

  compileOnly("org.eclipse.microprofile.openapi:microprofile-openapi-api")
  compileOnly("jakarta.validation:jakarta.validation-api")
  compileOnly("com.fasterxml.jackson.core:jackson-annotations")
  compileOnly("com.google.code.findbugs:jsr305")

  runtimeOnly("com.h2database:h2")
  runtimeOnly("org.postgresql:postgresql")

  testImplementation(nessieProjectPlatform("nessie-deps-testing", gradle))
  testCompileOnly(platform("com.fasterxml.jackson:jackson-bom"))
  testImplementation(platform("org.junit:junit-bom"))
  testCompileOnly(nessieProjectPlatform("nessie-deps-build-only", gradle))
  testAnnotationProcessor(nessieProjectPlatform("nessie-deps-build-only", gradle))

  testImplementation(nessieProject("nessie-jaxrs-testextension"))

  testRuntimeOnly("ch.qos.logback:logback-classic")

  testCompileOnly("org.immutables:value-annotations")
  testAnnotationProcessor("org.immutables:value-processor")

  testCompileOnly("com.fasterxml.jackson.core:jackson-annotations")
  testCompileOnly("org.eclipse.microprofile.openapi:microprofile-openapi-api")

  testImplementation("org.assertj:assertj-core")
  testImplementation("org.junit.jupiter:junit-jupiter-api")
  testImplementation("org.junit.jupiter:junit-jupiter-params")
  testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine")
}

tasks.named<ProcessResources>("processResources") {
  inputs.property("nessieVersion", project.version)
  expand("nessieVersion" to project.version)
}

tasks.named<Test>("test") { systemProperty("expectedNessieVersion", project.version) }

val mainClassName = "org.projectnessie.gc.tool.cli.CLI"

val generateAutoComplete by
  tasks.creating(JavaExec::class.java) {
    group = "build"
    description = "Generates the bash/zsh autocompletion scripts"

    val compileJava = tasks.named<JavaCompile>("compileJava")

    dependsOn(compileJava)

    val completionScriptsDir = project.buildDir.resolve("classes/java/main/META-INF/completion")

    doFirst { completionScriptsDir.mkdirs() }

    mainClass.set("picocli.AutoComplete")
    classpath(configurations.named("runtimeClasspath"), compileJava)
    args(
      "--force",
      "-o",
      completionScriptsDir.resolve("nessie-gc-completion").toString(),
      mainClassName
    )

    inputs.files("src/main").withPathSensitivity(PathSensitivity.RELATIVE)
    outputs.dir(completionScriptsDir)
  }

// generateAutoComplete writes the bash/zsh completion script into the main resource output,
// which is a bit ugly, but works. But the following tasks need a dependency to that task so that
// Gradle can properly evaluate the dependencies.
listOf("compileTestJava", "jandexMain", "jar", "shadowJar").forEach { t ->
  tasks.named(t) { dependsOn(generateAutoComplete) }
}

val shadowJar = tasks.named<ShadowJar>("shadowJar")

val unixExecutable by
  tasks.registering {
    group = "build"
    description = "Generates the Unix executable"

    dependsOn(shadowJar)
    val dir = buildDir.resolve("executable")
    val executable = dir.resolve("nessie-gc")
    inputs.files(shadowJar.get().archiveFile).withPathSensitivity(PathSensitivity.RELATIVE)
    outputs.files(executable)
    outputs.cacheIf { false } // very big file
    doFirst {
      dir.mkdirs()
      executable.outputStream().use { out ->
        projectDir.resolve("src/exec/exec-preamble.sh").inputStream().use { i -> i.transferTo(out) }
        shadowJar.get().archiveFile.get().asFile.inputStream().use { i -> i.transferTo(out) }
      }
      executable.setExecutable(true)
    }
  }

shadowJar {
  manifest { attributes["Main-Class"] = mainClassName }
  finalizedBy(unixExecutable)
  outputs.cacheIf { false } // very big jar
}

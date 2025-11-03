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
  id("nessie-conventions-java11")
  alias(libs.plugins.jmh)
}

val congocc by configurations.creating
val syntaxGen by configurations.creating

configurations.compileOnly { extendsFrom(syntaxGen) }

configurations.testFixturesApi { extendsFrom(syntaxGen) }

dependencies {
  compileOnly(libs.jakarta.annotation.api)
  compileOnly(project(":nessie-immutables-std"))
  annotationProcessor(project(":nessie-immutables-std", configuration = "processor"))

  implementation(libs.guava)
  implementation(libs.slf4j.api)

  syntaxGen(libs.congocc)
  syntaxGen(libs.jline)

  congocc(libs.congocc)

  testFixturesApi(platform(libs.junit.bom))
  testFixturesApi(libs.bundles.junit.testing)
  testFixturesApi(libs.jakarta.annotation.api)
  testFixturesApi(project(":nessie-immutables-std"))

  testFixturesRuntimeOnly(libs.logback.classic)

  jmhImplementation(libs.jmh.core)
  jmhAnnotationProcessor(libs.jmh.generator.annprocess)
}

abstract class Generate : JavaExec() {
  init {
    outputs.cacheIf { true }
  }

  @get:InputDirectory
  @get:PathSensitive(PathSensitivity.RELATIVE)
  abstract val sourceDir: DirectoryProperty

  @get:OutputDirectory abstract val outputDir: DirectoryProperty
}

val genNessieCliGrammarDir = project.layout.buildDirectory.dir("generated/sources/congocc/nessie")
val genNessieCliSyntaxDir =
  project.layout.buildDirectory.dir("generated/resources/nessie-cli-syntax")
val genNessieSparkSyntaxDir =
  project.layout.buildDirectory.dir("generated/resources/nessie-spark-syntax")
val genJsonGrammarDir = project.layout.buildDirectory.dir("generated/sources/congocc/json")

val generateNessieGrammar by
  tasks.registering(Generate::class) {
    sourceDir = projectDir.resolve("src/main/congocc/nessie")
    val sourceFile =
      sourceDir.get().file("nessie-cli-java.ccc").asFile.relativeTo(projectDir).toString()
    outputDir = genNessieCliGrammarDir

    classpath(congocc)

    doFirst { delete(genNessieCliGrammarDir) }

    mainClass = "org.congocc.app.Main"
    workingDir(projectDir)

    argumentProviders.add(
      CommandLineArgumentProvider {
        val base =
          listOf("-d", genNessieCliGrammarDir.get().asFile.toString(), "-jdk17", "-n", sourceFile)
        if (logger.isInfoEnabled) base else (base + listOf("-q"))
      }
    )
  }

val generateJsonGrammar by
  tasks.registering(Generate::class) {
    sourceDir = projectDir.resolve("src/main/congocc/json")
    outputDir = genJsonGrammarDir

    classpath(congocc)

    doFirst { delete(genJsonGrammarDir) }

    mainClass = "org.congocc.app.Main"
    workingDir(projectDir)

    argumentProviders.add(
      CommandLineArgumentProvider {
        val base =
          listOf(
            "-d",
            genJsonGrammarDir.get().asFile.toString(),
            "-jdk17",
            "-n",
            sourceDir.get().file("jsonc.ccc").asFile.relativeTo(projectDir).toString(),
          )
        if (logger.isInfoEnabled) base else (base + listOf("-q"))
      }
    )
  }

val compileJava =
  tasks.named("compileJava") { dependsOn(generateNessieGrammar, generateJsonGrammar) }

val generateNessieCliSyntax by
  tasks.registering(Generate::class) {
    dependsOn(compileJava)

    sourceDir = projectDir.resolve("src/main/congocc/nessie")
    outputDir = genNessieCliSyntaxDir

    classpath(syntaxGen, configurations.runtimeClasspath, compileJava)

    doFirst { delete(genNessieCliSyntaxDir) }

    mainClass = "org.projectnessie.nessie.cli.syntax.SyntaxTool"
    workingDir(projectDir)
    argumentProviders.add(
      CommandLineArgumentProvider {
        listOf(
          genNessieCliSyntaxDir.get().dir("org/projectnessie/nessie/cli/syntax").asFile.toString(),
          sourceDir.get().file("nessie-cli-java.ccc").asFile.relativeTo(projectDir).toString(),
          "omitSparkSql=true",
        )
      }
    )
  }

val generateNessieSparkSyntax by
  tasks.registering(Generate::class) {
    dependsOn(compileJava)

    sourceDir = projectDir.resolve("src/main/congocc/nessie")
    outputDir = genNessieSparkSyntaxDir

    classpath(syntaxGen, configurations.runtimeClasspath, compileJava)

    doFirst { delete(genNessieSparkSyntaxDir) }

    mainClass = "org.projectnessie.nessie.cli.syntax.SyntaxTool"
    workingDir(projectDir)
    argumentProviders.add(
      CommandLineArgumentProvider {
        listOf(
          genNessieSparkSyntaxDir
            .get()
            .dir("org/projectnessie/nessie/cli/spark-syntax")
            .asFile
            .toString(),
          sourceDir.get().file("nessie-cli-java.ccc").asFile.relativeTo(projectDir).toString(),
        )
      }
    )
  }

tasks.named("processResources") { dependsOn(generateNessieCliSyntax, generateNessieSparkSyntax) }

tasks.named("sourcesJar") { dependsOn(generateNessieCliSyntax, generateNessieSparkSyntax) }

sourceSets {
  main {
    java {
      srcDir(genNessieCliGrammarDir)
      srcDir(genJsonGrammarDir)
    }
    resources {
      srcDir(genNessieCliSyntaxDir)
      srcDir(genNessieSparkSyntaxDir)
    }
  }
}

tasks.withType<Checkstyle>().configureEach {
  // Cannot exclude build/ as a "general configuration", because the Checstyle task creates an
  // ant script behind the scenes, and that only supports "string" pattern matching using.
  // The base directories are the source directories, so all patterns match against paths
  // relative to a source-directory, not against full path names, not even relative to the current
  // project.
  exclude(
    "org/projectnessie/nessie/cli/grammar/*",
    "org/projectnessie/nessie/cli/sparkgrammar/*",
    "org/projectnessie/nessie/cli/jsongrammar/*",
  )
}

jmh { jmhVersion = libs.versions.jmh.get() }

tasks.named<Jar>("jmhJar") { manifest { attributes["Multi-Release"] = "true" } }

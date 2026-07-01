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
  id("nessie-conventions-java17")
  alias(libs.plugins.jmh)
}

val congocc = configurations.create("congocc")
val syntaxGen = configurations.create("syntaxGen")

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

val generateNessieGrammar =
  tasks.register<Generate>("generateNessieGrammar") {
    val genNessieCliGrammarDir = layout.buildDirectory.dir("generated/sources/congocc/nessie")
    val prjDir = layout.projectDirectory

    sourceDir = prjDir.dir("src/main/congocc/nessie")

    outputDir = genNessieCliGrammarDir

    classpath(congocc)

    doFirst { genNessieCliGrammarDir.get().asFile.deleteRecursively() }

    mainClass = "org.congocc.app.Main"
    workingDir(prjDir)

    argumentProviders.add(
      CommandLineArgumentProvider {
        val sourceFile =
          sourceDir.file("nessie-cli-java.ccc").get().asFile.relativeTo(prjDir.asFile)
        val base =
          listOf(
            "-d",
            genNessieCliGrammarDir.get().asFile.toString(),
            "-jdk17",
            "-n",
            sourceFile.toString(),
          )
        if (logger.isInfoEnabled) base else (base + listOf("-q"))
      }
    )
  }

val generateJsonGrammar =
  tasks.register<Generate>("generateJsonGrammar") {
    val genJsonGrammarDir = layout.buildDirectory.dir("generated/sources/congocc/json")

    val prjDir = layout.projectDirectory

    sourceDir = prjDir.dir("src/main/congocc/json")
    outputDir = genJsonGrammarDir

    classpath(congocc)

    doFirst { genJsonGrammarDir.get().asFile.deleteRecursively() }

    mainClass = "org.congocc.app.Main"
    workingDir(prjDir)

    argumentProviders.add(
      CommandLineArgumentProvider {
        val base =
          listOf(
            "-d",
            genJsonGrammarDir.get().asFile.toString(),
            "-jdk17",
            "-n",
            sourceDir.file("jsonc.ccc").get().asFile.relativeTo(prjDir.asFile).toString(),
          )
        if (logger.isInfoEnabled) base else (base + listOf("-q"))
      }
    )
  }

val compileJava =
  tasks.named("compileJava") { dependsOn(generateNessieGrammar, generateJsonGrammar) }

val generateNessieCliSyntax =
  tasks.register<Generate>("generateNessieCliSyntax") {
    val genNessieCliSyntaxDir = layout.buildDirectory.dir("generated/resources/nessie-cli-syntax")

    val prjDir = layout.projectDirectory

    dependsOn(compileJava)

    sourceDir = prjDir.dir("src/main/congocc/nessie")
    outputDir = genNessieCliSyntaxDir

    classpath(syntaxGen, configurations.runtimeClasspath, compileJava)

    doFirst { genNessieCliSyntaxDir.get().asFile.deleteRecursively() }

    mainClass = "org.projectnessie.nessie.cli.syntax.SyntaxTool"
    workingDir(projectDir)
    argumentProviders.add(
      CommandLineArgumentProvider {
        listOf(
          genNessieCliSyntaxDir.get().dir("org/projectnessie/nessie/cli/syntax").asFile.toString(),
          sourceDir.get().file("nessie-cli-java.ccc").asFile.relativeTo(prjDir.asFile).toString(),
          "omitSparkSql=true",
        )
      }
    )
  }

val generateNessieSparkSyntax =
  tasks.register<Generate>("generateNessieSparkSyntax") {
    val genNessieSparkSyntaxDir =
      layout.buildDirectory.dir("generated/resources/nessie-spark-syntax")

    val prjDir = layout.projectDirectory

    dependsOn(compileJava)

    sourceDir = prjDir.dir("src/main/congocc/nessie")
    outputDir = genNessieSparkSyntaxDir

    classpath(syntaxGen, configurations.runtimeClasspath, compileJava)

    doFirst { genNessieSparkSyntaxDir.get().asFile.deleteRecursively() }

    mainClass = "org.projectnessie.nessie.cli.syntax.SyntaxTool"
    workingDir(prjDir)
    argumentProviders.add(
      CommandLineArgumentProvider {
        listOf(
          genNessieSparkSyntaxDir
            .get()
            .dir("org/projectnessie/nessie/cli/spark-syntax")
            .asFile
            .toString(),
          sourceDir.get().file("nessie-cli-java.ccc").asFile.relativeTo(prjDir.asFile).toString(),
        )
      }
    )
  }

tasks.named("processResources") { dependsOn(generateNessieCliSyntax, generateNessieSparkSyntax) }

tasks.named("sourcesJar") { dependsOn(generateNessieCliSyntax, generateNessieSparkSyntax) }

sourceSets {
  main {
    java {
      srcDir(generateNessieGrammar)
      srcDir(generateJsonGrammar)
    }
    resources {
      srcDir(generateNessieCliSyntax)
      srcDir(generateNessieSparkSyntax)
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

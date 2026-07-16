/*
 * Copyright (C) 2024 Dremio
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

import docgen.GenerateCliGrammarMarkdown
import docgen.GenerateJavaHelpMarkdown
import docgen.GenerateJavaJarHelpMarkdown
import docgen.GenerateMarkdownDocs

plugins {
  `java-library`
}

val genProjects = configurations.create("genProjects")
val genSources = configurations.create("genSources")
val cliGrammar = configurations.create("cliGrammar")
val doclet = configurations.create("doclet")
val gcRunner = configurations.create("gcRunner")
val cliRunner = configurations.create("cliRunner")
val serverAdminRunner = configurations.create("serverAdminRunner")

val genProjectPaths = listOf(
  ":nessie-model",
  ":nessie-client",
  ":nessie-quarkus-common",
  ":nessie-quarkus-config",
  ":nessie-quarkus-authn",
  ":nessie-quarkus-authz",
  ":nessie-services-config",
  ":nessie-versioned-storage-bigtable",
  ":nessie-versioned-storage-cassandra",
  ":nessie-versioned-storage-cassandra2",
  ":nessie-versioned-storage-common",
  ":nessie-versioned-storage-dynamodb",
  ":nessie-versioned-storage-dynamodb2",
  ":nessie-versioned-storage-inmemory",
  ":nessie-versioned-storage-jdbc",
  ":nessie-versioned-storage-jdbc2",
  ":nessie-versioned-storage-mongodb",
  ":nessie-versioned-storage-mongodb2",
  ":nessie-versioned-storage-rocksdb",
  ":nessie-catalog-files-api",
  ":nessie-catalog-files-impl",
  ":nessie-catalog-service-config"
)

dependencies {
  doclet(project(":nessie-doc-generator-doclet"))
  doclet(project(":nessie-doc-generator-annotations"))
  doclet(libs.smallrye.config.core)

  genProjects(project(":nessie-doc-generator-annotations"))

  genProjectPaths.forEach { p ->
    genProjects(project(p))
    genSources(project(p, "mainSourceElements"))
  }

  cliGrammar(project(":nessie-cli-grammar")) {
    setTransitive(false)
  }

  cliRunner(project(":nessie-cli"))

  gcRunner(nessieProject("nessie-gc-tool"))

  serverAdminRunner(project(":nessie-server-admin-tool", "quarkusRunner"))
}

val generatedMarkdownDocsDir = layout.buildDirectory.dir("generatedMarkdownDocs")

val generatedMarkdownDocs = tasks.register<GenerateMarkdownDocs>("generatedMarkdownDocs") {
  mainClass.set("org.projectnessie.nessie.docgen.DocGenTool")
  toolClasspath.from(doclet)
  classpathArtifacts.from(genProjects)
  sourceArtifacts.from(genSources)
  destinationDirectory.set(generatedMarkdownDocsDir)
  verbose.set(logger.isInfoEnabled)
}

val cliHelpDir = layout.buildDirectory.dir("cliHelp")

val cliHelp = tasks.register<GenerateJavaHelpMarkdown>("cliHelp") {
  runtimeClasspath.from(cliRunner)
  mainClass.set("org.projectnessie.nessie.cli.cli.NessieCliMain")
  arguments.set(listOf("--help", "--non-ansi"))
  outputFile.set(cliHelpDir.map { it.file("cli-help.md") })
}

val gcHelpDir = layout.buildDirectory.dir("gcHelp")

val gcHelp = tasks.register<Sync>("gcHelp") {
  into(gcHelpDir)
}

for (cmdArgs in listOf(
  listOf("help", "--help")) +
  listOf("mark",
    "sweep",
    "gc",
    "list",
    "delete",
    "list-deferred",
    "deferred-deletes",
    "show",
    "show-sql-create-schema-script",
    "create-sql-schema",
    "completion-script",
    "show-licenses").map { cmd -> listOf("help-$cmd", "help", cmd) }
) {
  val name = cmdArgs[0]
  val t = tasks.register<GenerateJavaHelpMarkdown>("gc-$name")
  t.configure {
    runtimeClasspath.from(gcRunner)
    mainClass.set("org.projectnessie.gc.tool.cli.CLI")
    arguments.set(cmdArgs.subList(1, cmdArgs.size))
    outputFile.set(layout.buildDirectory.file("gc-$name/gc-$name.md"))
  }
  gcHelp.configure { from(t.flatMap { it.outputFile }) }
}

val serverAdminHelpDir = layout.buildDirectory.dir("serverAdminHelp")

val serverAdminHelp = tasks.register<Sync>("serverAdminHelp") {
  into(serverAdminHelpDir)
}

val serverAdminExecutableJar = layout.file(serverAdminRunner.elements.map { it.single().asFile })
val java21Launcher = javaToolchains.launcherFor {
  languageVersion.set(JavaLanguageVersion.of(21))
}
var previousServerAdminHelp: TaskProvider<GenerateJavaJarHelpMarkdown>? = null

for (cmdArgs in listOf(
  listOf("help", "help")) +
  listOf(
      "info",
      "check-content",
      "delete-catalog-tasks",
      "cleanup-repository",
      "erase-repository",
      "export",
      "import",
      "show-licenses").map { cmd -> listOf("help-$cmd", "help", cmd) }
) {
  val name = cmdArgs[0]
  val previous = previousServerAdminHelp
  val t = tasks.register<GenerateJavaJarHelpMarkdown>("serverAdmin-$name")
  t.configure {
    executableJar.set(serverAdminExecutableJar)
    javaLauncher.set(java21Launcher)
    arguments.set(cmdArgs.subList(1, cmdArgs.size))
    outputFile.set(layout.buildDirectory.file("serverAdmin-$name/serverAdmin-$name.md"))
    previous?.let { mustRunAfter(it) }
  }
  previousServerAdminHelp = t
  serverAdminHelp.configure { from(t.flatMap { it.outputFile }) }
}

val cliGrammarDocsDir = layout.buildDirectory.dir("cliGrammarDocs")

val cliGrammarDocs = tasks.register<GenerateCliGrammarMarkdown>("cliGrammarDocs") {
  cliGrammarArchive.set(layout.file(cliGrammar.elements.map { it.single().asFile }))
  outputDirectory.set(cliGrammarDocsDir)
}

tasks.register<Sync>("generateDocs") {
  dependsOn(generatedMarkdownDocs)
  dependsOn(cliHelp)
  dependsOn(gcHelp)
  dependsOn(serverAdminHelp)
  dependsOn(cliGrammarDocs)

  val targetDir = layout.buildDirectory.dir("markdown-docs")

  outputs.dir(targetDir)

  into(targetDir)

  from(generatedMarkdownDocs.flatMap { it.destinationDirectory })
  from(cliHelp.flatMap { it.outputFile })
  from(gcHelpDir)
  from(serverAdminHelpDir)
  from(cliGrammarDocs.flatMap { it.outputDirectory })

  duplicatesStrategy = DuplicatesStrategy.FAIL
}

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

import java.io.ByteArrayOutputStream
import java.io.InputStream

plugins {
  `java-library`
}

val genProjects by configurations.creating
val genSources by configurations.creating
val cliGrammar by configurations.creating
val doclet by configurations.creating
val gcRunner by configurations.creating
val cliRunner by configurations.creating
val serverAdminRunner by configurations.creating

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

val generatedMarkdownDocs = tasks.register<JavaExec>("generatedMarkdownDocs") {

  mainClass = "org.projectnessie.nessie.docgen.DocGenTool"

  outputs.cacheIf { true }
  outputs.dir(generatedMarkdownDocsDir)
  inputs.files(doclet)
  inputs.files(genProjects)
  inputs.files(genSources)

  doFirst {
    delete(generatedMarkdownDocsDir)
  }

  argumentProviders.add(CommandLineArgumentProvider {

    // So, in theory, all 'org.gradle.category' attributes should use the type
    // org.gradle.api.attributes.Category,
    // as Category.CATEGORY_ATTRIBUTE is defined. BUT! Some attributes have an attribute type ==
    // String.class!
    val categoryAttributeAsString = Attribute.of("org.gradle.category", String::class.java)

    val classes = genProjects.incoming.artifacts
      .filter { a ->
        // dependencies:
        //  org.gradle.category=library
        val category =
          a.variant.attributes.getAttribute(Category.CATEGORY_ATTRIBUTE)
            ?: a.variant.attributes.getAttribute(categoryAttributeAsString)
        category != null && category.toString() == Category.LIBRARY
      }
      .map { a -> a.file }

    val sources = genSources.incoming.artifacts
      .filter { a ->
        // sources:
        //  org.gradle.category=verification
        //  org.gradle.verificationtype=main-sources

        val category = a.variant.attributes.getAttribute(Category.CATEGORY_ATTRIBUTE)
        val verificationType =
          a.variant.attributes.getAttribute(VerificationType.VERIFICATION_TYPE_ATTRIBUTE)
        category != null &&
          category.name == Category.VERIFICATION &&
          verificationType != null &&
          verificationType.name == VerificationType.MAIN_SOURCES &&
          a.file.name != "resources"
      }
      .map { a -> a.file }

    listOf(
      "--classpath", classes.joinToString(":"),
      "--sourcepath", sources.joinToString(":"),
      "--destination", generatedMarkdownDocsDir.get().toString()
    ) + (if (logger.isInfoEnabled) listOf("--verbose") else listOf())
  })

  classpath(doclet)
}

val cliHelpDir = layout.buildDirectory.dir("cliHelp")

val cliHelp by tasks.registering(JavaExec::class) {
  mainClass = "-jar"

  inputs.files(cliRunner)
  outputs.cacheIf { true }
  outputs.dir(cliHelpDir)

  classpath(cliRunner)

  mainClass = "org.projectnessie.nessie.cli.cli.NessieCliMain"
  args("--help", "--non-ansi")

  doFirst {
    delete(cliHelpDir)
  }

  standardOutput = ByteArrayOutputStream()

  doLast {
    cliHelpDir.get().asFile.mkdirs()

    file(cliHelpDir.get().file("cli-help.md")).writeText("```\n$standardOutput\n```\n")
  }
}

val gcHelpDir = layout.buildDirectory.dir("gcHelp")

val gcHelp by tasks.registering(Sync::class) {
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
  val t = tasks.register<JavaExec>("gc-$name")
  t.configure {
    inputs.files(gcRunner)
    val dir = layout.buildDirectory.dir("gc-$name")
    outputs.cacheIf { true }
    outputs.dir(dir)

    classpath(gcRunner)

    val gcMainClass = "org.projectnessie.gc.tool.cli.CLI"

    mainClass = gcMainClass
    args(cmdArgs.subList(1, cmdArgs.size))

    standardInput = InputStream.nullInputStream()
    standardOutput = ByteArrayOutputStream()

    doLast {
      dir.get().asFile.mkdirs()
      file(dir.get().file("gc-$name.md")).writeText("```\n$standardOutput\n```\n")
    }
  }
  gcHelp.configure { from(t) }
}

val serverAdminHelpDir = layout.buildDirectory.dir("serverAdminHelp")

val serverAdminHelp by tasks.registering(Sync::class) {
  into(serverAdminHelpDir)
}

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
  val t = tasks.register<JavaExec>("serverAdmin-$name")
  t.configure {
    inputs.files(serverAdminRunner)
    val dir = layout.buildDirectory.dir("serverAdmin-$name")
    outputs.cacheIf { true }
    outputs.dir(dir)

    classpath(serverAdminRunner)

    args(cmdArgs.subList(1, cmdArgs.size))

    standardInput = InputStream.nullInputStream()
    standardOutput = ByteArrayOutputStream()

    doLast {
      dir.get().asFile.mkdirs()
      file(dir.get().file("serverAdmin-$name.md")).writeText("```\n$standardOutput\n```\n")
    }
  }
  serverAdminHelp.configure { from(t) }
}

tasks.register<Sync>("generateDocs") {
  dependsOn(generatedMarkdownDocs)
  dependsOn(cliHelp)
  dependsOn(gcHelp)
  dependsOn(serverAdminHelp)

  val targetDir = layout.buildDirectory.dir("markdown-docs")

  inputs.files(cliGrammar)
  outputs.dir(targetDir)

  into(targetDir)

  from(generatedMarkdownDocsDir)
  from(cliHelpDir)
  from(gcHelpDir)
  from(serverAdminHelpDir)
  from(provider { zipTree(cliGrammar.singleFile) }) {
    include("org/projectnessie/nessie/cli/syntax/*.help.txt")
    include("org/projectnessie/nessie/cli/syntax/*.md")
    eachFile { path = if (name.endsWith(".help.txt")) "cli-help-${name.replace(".help.txt", ".md")}" else "cli-syntax-$name" }
  }
  from(provider { zipTree(cliGrammar.singleFile) }) {
    include("org/projectnessie/nessie/cli/spark-syntax/*.help.txt")
    include("org/projectnessie/nessie/cli/spark-syntax/*.md")
    eachFile { path = if (name.endsWith(".help.txt")) "cli-help-${name.replace(".help.txt", ".md")}" else "spark-sql-syntax-$name" }
  }

  duplicatesStrategy = DuplicatesStrategy.FAIL

  doLast { delete(targetDir.get().dir("org")) }
}

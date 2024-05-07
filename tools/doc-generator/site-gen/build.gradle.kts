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

plugins {
  `java-library`
}

extra["maven.name"] = "Generates markdown for the projectnessie site for client + server documentation"

val genProjects by configurations.creating
val genSources by configurations.creating
val cliGrammar by configurations.creating
val doclet by configurations.creating
val cliRunner by configurations.creating

val genProjectPaths = listOf(
  ":nessie-model",
  ":nessie-client",
  ":nessie-quarkus-common",
  ":nessie-quarkus-auth",
  ":nessie-services-config",
  ":nessie-versioned-storage-bigtable",
  ":nessie-versioned-storage-cassandra",
  ":nessie-versioned-storage-common",
  ":nessie-versioned-storage-dynamodb",
  ":nessie-versioned-storage-inmemory",
  ":nessie-versioned-storage-jdbc",
  ":nessie-versioned-storage-mongodb",
  ":nessie-versioned-storage-rocksdb"
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
}

val generatedMarkdownDocsDir = layout.buildDirectory.dir("generatedMarkdownDocs")

val generatedMarkdownDocs = tasks.register<JavaExec>("generatedMarkdownDocs") {

  mainClass = "org.projectnessie.nessie.docgen.DocGenTool"

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

val cliHelp = tasks.register<JavaExec>("cliHelp") {
  mainClass = "-jar"

  inputs.files(cliRunner)
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

tasks.register<Copy>("generateDocs") {
  dependsOn(generatedMarkdownDocs)
  dependsOn(cliHelp)

  val targetDir = layout.buildDirectory.dir("markdown-docs")

  doFirst {
    delete(targetDir)
  }

  inputs.files(cliGrammar)
  outputs.dir(targetDir)

  into(targetDir)

  from(generatedMarkdownDocsDir)
  from(cliHelpDir)
  from(provider { zipTree(cliGrammar.singleFile) }) {
    include("org/projectnessie/nessie/cli/syntax/*.md")
    eachFile {
      path = "cli-syntax-$name"
    }
  }
  from(provider { zipTree(cliGrammar.singleFile) }) {
    include("org/projectnessie/nessie/cli/syntax/*.help.txt")
    eachFile {
      path = "cli-help-${name.replace(".help.txt", ".md")}"
    }
  }

  doLast {
    delete(targetDir.get().dir("org"))
  }
}

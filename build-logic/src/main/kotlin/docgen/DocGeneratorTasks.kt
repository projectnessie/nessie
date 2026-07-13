/*
 * Copyright (C) 2026 Dremio
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

package docgen

import java.io.ByteArrayOutputStream
import java.io.File
import java.util.zip.ZipFile
import javax.inject.Inject
import org.gradle.api.DefaultTask
import org.gradle.api.GradleException
import org.gradle.api.file.ConfigurableFileCollection
import org.gradle.api.file.DirectoryProperty
import org.gradle.api.file.RegularFileProperty
import org.gradle.api.provider.ListProperty
import org.gradle.api.provider.Property
import org.gradle.api.tasks.CacheableTask
import org.gradle.api.tasks.Classpath
import org.gradle.api.tasks.IgnoreEmptyDirectories
import org.gradle.api.tasks.Input
import org.gradle.api.tasks.InputFile
import org.gradle.api.tasks.InputFiles
import org.gradle.api.tasks.Nested
import org.gradle.api.tasks.OutputDirectory
import org.gradle.api.tasks.OutputFile
import org.gradle.api.tasks.PathSensitive
import org.gradle.api.tasks.PathSensitivity
import org.gradle.api.tasks.TaskAction
import org.gradle.jvm.toolchain.JavaLauncher
import org.gradle.process.ExecOperations

@CacheableTask
abstract class GenerateJavaHelpMarkdown : DefaultTask() {
  @get:Classpath abstract val runtimeClasspath: ConfigurableFileCollection

  @get:Input abstract val mainClass: Property<String>

  @get:Input abstract val arguments: ListProperty<String>

  @get:OutputFile abstract val outputFile: RegularFileProperty

  @get:Inject abstract val execOperations: ExecOperations

  @TaskAction
  fun generate() {
    val output = ByteArrayOutputStream()
    execOperations.javaexec {
      classpath = runtimeClasspath
      mainClass.set(this@GenerateJavaHelpMarkdown.mainClass)
      args(this@GenerateJavaHelpMarkdown.arguments.get())
      standardOutput = output
    }
    writeMarkdown(output)
  }

  private fun writeMarkdown(output: ByteArrayOutputStream) {
    val file = outputFile.get().asFile
    file.parentFile.mkdirs()
    file.writeText("```\n${output.toString(Charsets.UTF_8)}\n```\n", Charsets.UTF_8)
  }
}

@CacheableTask
abstract class GenerateJavaJarHelpMarkdown : DefaultTask() {
  @get:InputFile
  @get:PathSensitive(PathSensitivity.RELATIVE)
  abstract val executableJar: RegularFileProperty

  @get:Input abstract val arguments: ListProperty<String>

  @get:OutputFile abstract val outputFile: RegularFileProperty

  @get:Nested abstract val javaLauncher: Property<JavaLauncher>

  @get:Inject abstract val execOperations: ExecOperations

  @TaskAction
  fun generate() {
    val output = ByteArrayOutputStream()
    execOperations.exec {
      executable = javaLauncher.get().executablePath.asFile.absolutePath
      args("-jar", executableJar.get().asFile.absolutePath)
      args(this@GenerateJavaJarHelpMarkdown.arguments.get())
      standardOutput = output
    }

    val file = outputFile.get().asFile
    file.parentFile.mkdirs()
    file.writeText("```\n${output.toString(Charsets.UTF_8)}\n```\n", Charsets.UTF_8)
  }
}

@CacheableTask
abstract class GenerateMarkdownDocs : DefaultTask() {
  @get:Classpath abstract val toolClasspath: ConfigurableFileCollection

  @get:Classpath abstract val classpathArtifacts: ConfigurableFileCollection

  @get:InputFiles
  @get:IgnoreEmptyDirectories
  @get:PathSensitive(PathSensitivity.RELATIVE)
  abstract val sourceArtifacts: ConfigurableFileCollection

  @get:Input abstract val mainClass: Property<String>

  @get:Input abstract val verbose: Property<Boolean>

  @get:OutputDirectory abstract val destinationDirectory: DirectoryProperty

  @get:Inject abstract val execOperations: ExecOperations

  @TaskAction
  fun generate() {
    val destination = destinationDirectory.get().asFile
    destination.deleteRecursively()
    destination.mkdirs()

    val args =
      listOf(
        "--classpath",
        classpathArtifacts.files.joinToString(File.pathSeparator),
        "--sourcepath",
        sourceArtifacts.files.filter { it.name != "resources" }.joinToString(File.pathSeparator),
        "--destination",
        destination.absolutePath,
      ) + if (verbose.get()) listOf("--verbose") else emptyList()

    execOperations.javaexec {
      classpath = toolClasspath
      mainClass.set(this@GenerateMarkdownDocs.mainClass)
      args(args)
    }
  }
}

@CacheableTask
abstract class GenerateCliGrammarMarkdown : DefaultTask() {
  @get:InputFile
  @get:PathSensitive(PathSensitivity.RELATIVE)
  abstract val cliGrammarArchive: RegularFileProperty

  @get:OutputDirectory abstract val outputDirectory: DirectoryProperty

  @TaskAction
  fun generate() {
    val output = outputDirectory.get().asFile
    output.deleteRecursively()
    output.mkdirs()

    ZipFile(cliGrammarArchive.get().asFile).use { zip ->
      zip
        .entries()
        .asSequence()
        .filter { entry -> !entry.isDirectory }
        .forEach { entry ->
          targetName(entry.name)?.let { targetName ->
            val target = output.resolve(targetName)
            if (target.exists()) {
              throw GradleException("Duplicate CLI grammar documentation output: $targetName")
            }
            zip.getInputStream(entry).use { input ->
              target.outputStream().use { input.copyTo(it) }
            }
          }
        }
    }
  }

  private fun targetName(path: String): String? {
    val syntax = "org/projectnessie/nessie/cli/syntax/"
    val sparkSyntax = "org/projectnessie/nessie/cli/spark-syntax/"
    return when {
      path.startsWith(syntax) -> {
        val name = path.removePrefix(syntax)
        when {
          name.endsWith(".help.txt") -> "cli-help-${name.replace(".help.txt", ".md")}"
          name.endsWith(".md") -> "cli-syntax-$name"
          else -> null
        }
      }
      path.startsWith(sparkSyntax) -> {
        val name = path.removePrefix(sparkSyntax)
        when {
          name.endsWith(".help.txt") -> "cli-help-${name.replace(".help.txt", ".md")}"
          name.endsWith(".md") -> "spark-sql-syntax-$name"
          else -> null
        }
      }
      else -> null
    }
  }
}

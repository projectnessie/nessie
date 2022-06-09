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

package org.projectnessie.buildtools.reflectionconfig

import java.io.BufferedInputStream
import java.io.File
import java.io.FileInputStream
import java.io.InputStream
import java.util.jar.JarInputStream
import java.util.regex.Pattern
import org.gradle.api.DefaultTask
import org.gradle.api.GradleException
import org.gradle.api.tasks.CacheableTask
import org.gradle.api.tasks.Input
import org.gradle.api.tasks.InputFiles
import org.gradle.api.tasks.OutputDirectory
import org.gradle.api.tasks.PathSensitive
import org.gradle.api.tasks.PathSensitivity
import org.gradle.api.tasks.TaskAction
import org.objectweb.asm.ClassReader
import org.objectweb.asm.ClassVisitor
import org.objectweb.asm.Opcodes
import org.objectweb.asm.Type

@CacheableTask
open class ReflectionConfigTask : DefaultTask() {
  @InputFiles
  @PathSensitive(PathSensitivity.RELATIVE)
  val classesFolder = project.objects.directoryProperty()

  @OutputDirectory val outputDirectory = project.objects.directoryProperty()

  @Input val classExtendsPatterns = project.objects.listProperty(String::class.java)

  @Input val classImplementsPatterns = project.objects.listProperty(String::class.java)

  @Input val setName = project.objects.property(String::class.java)

  @Input var includeConfigurations = project.objects.listProperty(String::class.java)

  @TaskAction
  fun generateReflectionConfig() {
    val extPats = classExtendsPatterns.get().map { s -> Pattern.compile(s) }.toList()
    val implPats = classImplementsPatterns.get().map { s -> Pattern.compile(s) }.toList()

    val baseDir =
      outputDirectory
        .get()
        .file("META-INF/native-image/${project.group}/${project.name}/${setName.get()}")
        .asFile
    if (!baseDir.isDirectory) {
      if (!baseDir.mkdirs()) {
        throw GradleException("Could not create directory '$baseDir'")
      }
    }

    val classFolderStream =
      classesFolder
        .get()
        .asFileTree
        .filter { f -> f.name.endsWith(".class") }
        .mapNotNull { file -> processClassFile(file, extPats, implPats) }

    val dependenciesStream =
      includeConfigurations
        .get()
        .map { cfg -> project.configurations.getByName(cfg) }
        .flatMap { cfg -> cfg.resolve() }
        .flatMap { file ->
          val classNames = mutableListOf<String>()
          JarInputStream(FileInputStream(file.absoluteFile)).use {
            while (true) {
              val n = it.nextJarEntry ?: break
              if (n.name.endsWith(".class")) {
                val clsName = processClassFile(it, extPats, implPats)
                if (clsName != null) {
                  classNames.add(clsName)
                }
              }
            }
          }
          classNames
        }

    val projectGroup = project.group
    val projectName = project.name
    val projectVersion = project.version

    baseDir
      .resolve("native-image.properties")
      .writeText(
        "# This file is generated for ${projectGroup}:${projectName}:${projectVersion}.\n" +
          "# Contains classes \n" +
          "#   with superclass: ${extPats.joinToString(",\n#     ", "\n#     ")}\n" +
          "#   implementing interfaces: ${implPats.joinToString(",\n#     ", "\n#     ")}\n" +
          "Args = -H:ReflectionConfigurationResources=\${.}/reflection-config.json\n"
      )

    baseDir
      .resolve("reflection-config.json")
      .writeText(
        (dependenciesStream + classFolderStream).joinToString(",\n", "[\n", "\n]") { clsName ->
          """  {
              |    "name" : "$clsName",
              |    "allDeclaredConstructors" : true,
              |    "allPublicConstructors" : true,
              |    "allDeclaredMethods" : true,
              |    "allPublicMethods" : true,
              |    "allDeclaredFields" : true,
              |    "allPublicFields" : true
              |  }""".trimMargin()
        }
      )
  }

  private fun processClassFile(
    file: File,
    extPats: List<Pattern>,
    implPats: List<Pattern>
  ): String? {
    BufferedInputStream(FileInputStream(file)).use { input ->
      return processClassFile(input, extPats, implPats)
    }
  }

  private fun processClassFile(
    input: InputStream,
    extPats: List<Pattern>,
    implPats: List<Pattern>
  ): String? {
    val classVisitor = ClsVisit()

    ClassReader(input)
      .accept(
        classVisitor,
        ClassReader.SKIP_CODE + ClassReader.SKIP_FRAMES + ClassReader.SKIP_DEBUG
      )

    if (classVisitor.extends != null &&
        (matchesPattern(classVisitor.extends!!, extPats) ||
          matchesPattern(classVisitor.implements, implPats))
    ) {
      return classVisitor.className
    }
    return null
  }

  private fun matchesPattern(ifs: List<String>, pats: List<Pattern>): Boolean {
    return ifs.any { ifName -> matchesPattern(ifName, pats) }
  }

  private fun matchesPattern(cls: String, pats: List<Pattern>): Boolean {
    if (pats.isEmpty()) {
      return true
    }
    return pats.any { p -> p.matcher(cls).matches() }
  }

  private class ClsVisit : ClassVisitor(Opcodes.ASM9) {
    var className: String? = null
    var extends: String? = null
    var implements: List<String> = listOf()

    override fun visit(
      version: Int,
      access: Int,
      name: String?,
      signature: String?,
      superName: String?,
      interfaces: Array<out String>
    ) {
      if (access.and(Opcodes.ACC_PUBLIC) != 0) {
        className = Type.getObjectType(name).className
        extends = Type.getObjectType(superName).className
        implements = interfaces.map { i -> Type.getObjectType(i).className }.toList()
      }
    }
  }
}

/*
 * Copyright (C) 2023 Dremio
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

import java.util.Properties
import net.ltgt.gradle.errorprone.CheckSeverity
import net.ltgt.gradle.errorprone.errorprone

plugins {
  eclipse
  id("org.jetbrains.gradle.plugin.idea-ext")
  checkstyle
  id("net.ltgt.errorprone")
}

// ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Checkstyle

// Exclude projects that only generate Java from protobuf
if (project.name.endsWith("-proto") || project.extra.has("duplicated-project-sources")) {
  tasks.withType<Checkstyle>().configureEach { enabled = false }
} else {
  checkstyle {
    toolVersion = libsRequiredVersion("checkstyle")
    config = MemoizedCheckstyleConfig.checkstyleConfig(rootProject)
    isShowViolations = true
    isIgnoreFailures = false
  }

  tasks.withType<Checkstyle>().configureEach {
    if (plugins.hasPlugin("io.quarkus")) {
      when (name) {
        "checkstyleMain" -> dependsOn(tasks.named("compileQuarkusGeneratedSourcesJava"))
        "checkstyleTestFixtures" -> dependsOn(tasks.named("compileQuarkusTestGeneratedSourcesJava"))
        "checkstyleTest" -> dependsOn(tasks.named("compileQuarkusTestGeneratedSourcesJava"))
        else -> {}
      }
    }
    maxWarnings = 0 // treats warnings as errors
  }

  val sourceSets = project.extensions.findByType(SourceSetContainer::class.java)
  if (sourceSets != null) {
    val checkstyleAll = tasks.register("checkstyle")
    checkstyleAll.configure { description = "Checkstyle all source sets" }

    sourceSets.withType(SourceSet::class.java).configureEach {
      val sourceSet = this
      val checkstyleTask = tasks.named(sourceSet.getTaskName("checkstyle", null))
      checkstyleTask.configure { dependsOn(sourceSet.getTaskName("process", "jandexIndex")) }
      checkstyleAll.configure { dependsOn(checkstyleTask) }
    }
  }
}

private class MemoizedCheckstyleConfig {
  companion object {
    fun checkstyleConfig(rootProject: Project): TextResource {
      val e = rootProject.extensions.getByType(ExtraPropertiesExtension::class)
      if (e.has("nessie-checkstyle-config")) {
        return e.get("nessie-checkstyle-config") as TextResource
      }
      val configResource = rootProject.resources.text.fromFile("codestyle/checkstyle-config.xml")
      e.set("nessie-checkstyle-config", configResource)
      return configResource
    }
  }
}

// ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Errorprone

tasks.withType<JavaCompile>().configureEach {
  if (!project.extra.has("duplicated-project-sources")) {
    options.errorprone.disableAllChecks.set(true)
  } else {
    options.errorprone.disableWarningsInGeneratedCode.set(true)

    val errorproneRules = rootProject.projectDir.resolve("codestyle/errorprone-rules.properties")
    inputs.file(errorproneRules).withPathSensitivity(PathSensitivity.RELATIVE)

    val checksMapProperty =
      objects
        .mapProperty(String::class.java, CheckSeverity::class.java)
        .convention(provider { MemoizedErrorproneRules.rules(rootProject, errorproneRules) })

    options.errorprone.checks.putAll(checksMapProperty)
    options.errorprone.excludedPaths.set(".*/build/[generated|tmp].*")
  }
}

private class MemoizedErrorproneRules {
  companion object {
    fun rules(rootProject: Project, rulesFile: File): Map<String, CheckSeverity> {
      if (rootProject.extra.has("nessieErrorproneRules")) {
        @Suppress("UNCHECKED_CAST")
        return rootProject.extra["nessieErrorproneRules"] as Map<String, CheckSeverity>
      }
      val checksMap =
        rulesFile.reader().use {
          val rules = Properties()
          rules.load(it)
          rules
            .mapKeys { e -> (e.key as String).trim() }
            .mapValues { e -> (e.value as String).trim() }
            .filter { e -> e.key.isNotEmpty() && e.value.isNotEmpty() }
            .mapValues { e -> CheckSeverity.valueOf(e.value) }
            .toMap()
        }
      rootProject.extra["nessieErrorproneRules"] = checksMap
      return checksMap
    }
  }
}

plugins.withType<JavaPlugin>().configureEach {
  configure<JavaPluginExtension> {
    sourceSets.configureEach {
      dependencies {
        add(
          "errorprone",
          "com.google.errorprone:error_prone_core:${libsRequiredVersion("errorprone")}"
        )
        add(
          "errorprone",
          "jp.skypencil.errorprone.slf4j:errorprone-slf4j:${libsRequiredVersion("errorproneSlf4j")}"
        )
      }
    }
  }
}

// ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// IDE

if (!System.getProperty("idea.sync.active").toBoolean()) {
  idea {
    module {
      // Do not index the following folders
      excludeDirs =
        excludeDirs +
          setOf(
            buildDir.resolve("libs"),
            buildDir.resolve("reports"),
            buildDir.resolve("test-results"),
            buildDir.resolve("classes"),
            buildDir.resolve("jacoco"),
            buildDir.resolve("jandex"),
            buildDir.resolve("quarkus-app"),
            buildDir.resolve("generated"),
            buildDir.resolve("docs"),
            buildDir.resolve("jacoco-report"),
            buildDir.resolve("openapi"),
            buildDir.resolve("openapi-extra"),
            buildDir.resolve("spotless"),
            buildDir.resolve("tmp")
          )
    }
  }
}

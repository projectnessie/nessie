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

val noCheckstyle =
  project.name.endsWith("-proto") || project.extra.has("duplicated-project-sources")

// Exclude projects that only generate Java from protobuf
if (noCheckstyle) {
  tasks.withType<Checkstyle>().configureEach { enabled = false }
} else {
  checkstyle {
    toolVersion = libsRequiredVersion("checkstyle")
    config = MemoizedCheckstyleConfig.checkstyleConfig(rootProject)
    isShowViolations = true
    isIgnoreFailures = false
  }

  configurations.configureEach {
    // Avoids dependency resolution error:
    // Could not resolve all task dependencies for configuration '...:checkstyle'.
    //   > Module 'com.google.guava:guava' has been rejected:
    //     Cannot select module with conflict on capability
    //         'com.google.collections:google-collections:33.0.0-jre'
    //         also provided by [com.google.collections:google-collections:1.0(runtime)]
    //   > Module 'com.google.collections:google-collections' has been rejected:
    //     Cannot select module with conflict on capability
    //         'com.google.collections:google-collections:1.0'
    //         also provided by [com.google.guava:guava:33.0.0-jre(jreRuntimeElements)]
    resolutionStrategy.capabilitiesResolution.withCapability(
      "com.google.collections:google-collections"
    ) {
      selectHighestVersion()
    }
  }

  tasks.withType<Checkstyle>().configureEach {
    if (plugins.hasPlugin("io.quarkus")) {
      when (name) {
        "checkstyleMain" -> dependsOn("compileQuarkusGeneratedSourcesJava")
        "checkstyleTestFixtures" -> dependsOn("compileQuarkusTestGeneratedSourcesJava")
        "checkstyleTest" -> dependsOn("compileQuarkusTestGeneratedSourcesJava")
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
      val checkstyleTask = sourceSet.getTaskName("checkstyle", null)
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
    options.errorprone.disableAllChecks = true
  } else {
    options.errorprone.disableWarningsInGeneratedCode = true

    val errorproneRules = rootProject.projectDir.resolve("codestyle/errorprone-rules.properties")
    inputs.file(errorproneRules).withPathSensitivity(PathSensitivity.RELATIVE)

    val checksMapProperty =
      objects
        .mapProperty(String::class.java, CheckSeverity::class.java)
        .convention(provider { MemoizedErrorproneRules.rules(rootProject, errorproneRules) })

    options.errorprone.checks.putAll(checksMapProperty)
    options.errorprone.excludedPaths = ".*/build/[generated|tmp].*"
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
          "com.google.errorprone:error_prone_core:${libsRequiredVersion("errorprone")}",
        )
        add(
          "errorprone",
          "jp.skypencil.errorprone.slf4j:errorprone-slf4j:${libsRequiredVersion("errorproneSlf4j")}",
        )
      }
    }
  }
}

// Adds Git/Build/System related information to the generated jars, if the `release` project
// property is present. Do not add that information in development builds, so that the
// generated jars are still cachable for Gradle.
if (project.hasProperty("release") || project.hasProperty("jarWithGitInfo")) {
  tasks.withType<Jar>().configureEach {
    manifest { MemoizedGitInfo.gitInfo(rootProject, attributes) }
  }
}

class MemoizedGitInfo {
  companion object {
    private fun execProc(rootProject: Project, cmd: String, vararg args: Any): String {
      var out =
        rootProject.providers
          .exec {
            executable = cmd
            args(args.toList())
          }
          .standardOutput
          .asText
          .get()
      return out.trim()
    }

    fun gitInfo(rootProject: Project, attribs: Attributes) {
      val props = gitInfo(rootProject)
      attribs.putAll(props)
    }

    fun gitInfo(rootProject: Project): Map<String, String> {
      if (!rootProject.hasProperty("release") && !rootProject.hasProperty("jarWithGitInfo")) {
        return emptyMap()
      }

      return if (rootProject.extra.has("gitReleaseInfo")) {
        @Suppress("UNCHECKED_CAST")
        rootProject.extra["gitReleaseInfo"] as Map<String, String>
      } else {
        val gitHead = execProc(rootProject, "git", "rev-parse", "HEAD")
        val gitDescribe = execProc(rootProject, "git", "describe", "--tags")
        val timestamp = execProc(rootProject, "date", "+%Y-%m-%d-%H:%M:%S%:z")
        val system = execProc(rootProject, "uname", "-a")
        val javaVersion = System.getProperty("java.version")

        val info =
          mapOf(
            "Nessie-Version" to
              rootProject.layout.projectDirectory.file("version.txt").asFile.readText().trim(),
            "Nessie-Build-Git-Head" to gitHead,
            "Nessie-Build-Git-Describe" to gitDescribe,
            "Nessie-Build-Timestamp" to timestamp,
            "Nessie-Build-System" to system,
            "Nessie-Build-Java-Version" to javaVersion,
          )
        rootProject.extra["gitReleaseInfo"] = info
        return info
      }
    }
  }
}

afterEvaluate {
  tasks.named("codeChecks").configure {
    dependsOn("spotlessCheck")
    if (!noCheckstyle) {
      dependsOn("checkstyle")
    }
    if (tasks.names.contains("checkLicense")) {
      dependsOn("checkLicense")
    }
  }
}

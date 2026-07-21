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

import java.io.ByteArrayOutputStream
import java.util.Properties
import net.ltgt.gradle.errorprone.CheckSeverity
import net.ltgt.gradle.errorprone.errorprone
import org.gradle.api.services.BuildService
import org.gradle.api.services.BuildServiceParameters
import org.gradle.process.ExecOperations

plugins {
  eclipse
  id("org.jetbrains.gradle.plugin.idea-ext")
  checkstyle
  id("net.ltgt.errorprone")
}

// ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Checkstyle

val noCheckstyle = project.name.endsWith("-proto") || noSourceCheckProjects.contains(project.path)

// Exclude projects that only generate Java from protobuf
if (noCheckstyle) {
  tasks.withType<Checkstyle>().configureEach { enabled = false }
} else {
  checkstyle {
    toolVersion = libsRequiredVersion("checkstyle")
    config =
      resources.text.fromFile(layout.settingsDirectory.file("codestyle/checkstyle-config.xml"))
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

if (noSourceCheckProjects.contains(project.path)) {
  tasks.withType<JavaCompile>().configureEach {
    options.compilerArgs.add("-XDaddTypeAnnotationsToSymbol=true")
    options.errorprone.disableAllChecks = true
  }
} else {
  tasks.withType<JavaCompile>().configureEach {
    options.compilerArgs.add("-XDaddTypeAnnotationsToSymbol=true")
    options.errorprone.disableWarningsInGeneratedCode = true

    val errorproneRules = layout.settingsDirectory.file("codestyle/errorprone-rules.properties")
    inputs.file(errorproneRules).withPathSensitivity(PathSensitivity.RELATIVE)

    val checksMapProperty =
      objects
        .mapProperty(String::class.java, CheckSeverity::class.java)
        .convention(provider { errorproneRules(errorproneRules.asFile) })

    options.errorprone.checks.putAll(checksMapProperty)
    options.errorprone.excludedPaths =
      ".*/build/(generated|tmp|classes/java/quarkus-generated-sources).*"
  }
}

private fun errorproneRules(rulesFile: File): Map<String, CheckSeverity> {
  return rulesFile.reader().use {
    val rules = Properties()
    rules.load(it)
    rules
      .mapKeys { e -> (e.key as String).trim() }
      .mapValues { e -> (e.value as String).trim() }
      .filter { e -> e.key.isNotEmpty() && e.value.isNotEmpty() }
      .mapValues { e -> CheckSeverity.valueOf(e.value) }
      .toMap()
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
if (
  providers.gradleProperty("release").isPresent ||
    providers.gradleProperty("jarWithGitInfo").isPresent
) {
  val gitBuildInfo =
    gradle.sharedServices.registerIfAbsent("gitBuildInfo", GitBuildInfoService::class.java) {
      parameters.versionFile.set(layout.settingsDirectory.file("version.txt"))
    }

  tasks.withType<Jar>().configureEach {
    usesService(gitBuildInfo)
    outputs.doNotCacheIf("Git build information intentionally reflects the current checkout") {
      true
    }
    outputs.upToDateWhen { false }
    inputs
      .file(gitBuildInfo.flatMap { it.parameters.versionFile })
      .withPathSensitivity(PathSensitivity.NONE)
    doFirst {
      manifest.attributes(gitBuildInfo.get().buildInfo())
    }
  }
}

abstract class GitBuildInfoService : BuildService<GitBuildInfoService.Parameters> {
  interface Parameters : BuildServiceParameters {
    val versionFile: RegularFileProperty
  }

  @get:Inject abstract val execOperations: ExecOperations

  private var buildInfo: Map<String, String>? = null

  @Synchronized
  fun buildInfo(): Map<String, String> {
    val existing = buildInfo
    if (existing != null) {
      return existing
    }

    val computed =
      mapOf(
        "Nessie-Version" to parameters.versionFile.get().asFile.readText(Charsets.UTF_8).trim(),
        "Nessie-Build-Git-Head" to execProc("git", "rev-parse", "HEAD"),
        "Nessie-Build-Git-Describe" to execProc("git", "describe", "--tags"),
        "Nessie-Build-Timestamp" to execProc("date", "+%Y-%m-%d-%H:%M:%S%:z"),
        "Nessie-Build-System" to execProc("uname", "-a"),
        "Nessie-Build-Java-Version" to System.getProperty("java.version"),
      )

    buildInfo = computed
    return computed
  }

  private fun execProc(cmd: String, vararg args: String): String {
    val output = ByteArrayOutputStream()
    execOperations.exec {
      executable = cmd
      args(*args)
      standardOutput = output
    }
    return output.toString(Charsets.UTF_8).trim()
  }
}

tasks.named("codeChecks").configure {
  dependsOn("spotlessCheck")
  if (!noCheckstyle) {
    dependsOn("checkstyle")
  }
  pluginManager.withPlugin("com.github.jk1.dependency-license-report") { dependsOn("checkLicense") }
}

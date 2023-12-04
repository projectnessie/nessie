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

import com.github.vlsi.jandex.JandexProcessResources
import java.io.File
import java.lang.IllegalArgumentException
import java.lang.IllegalStateException
import java.util.Properties
import org.gradle.api.Action
import org.gradle.api.DefaultTask
import org.gradle.api.JavaVersion
import org.gradle.api.Project
import org.gradle.api.Task
import org.gradle.api.artifacts.Dependency
import org.gradle.api.artifacts.ExternalModuleDependency
import org.gradle.api.artifacts.ModuleDependency
import org.gradle.api.artifacts.VersionCatalogsExtension
import org.gradle.api.file.FileTree
import org.gradle.api.file.RegularFileProperty
import org.gradle.api.invocation.Gradle
import org.gradle.api.logging.LogLevel
import org.gradle.api.resources.TextResource
import org.gradle.api.tasks.InputFile
import org.gradle.api.tasks.JavaExec
import org.gradle.api.tasks.OutputFile
import org.gradle.api.tasks.PathSensitive
import org.gradle.api.tasks.PathSensitivity
import org.gradle.api.tasks.SourceSetContainer
import org.gradle.api.tasks.TaskAction
import org.gradle.api.tasks.testing.Test
import org.gradle.jvm.toolchain.JavaLanguageVersion
import org.gradle.jvm.toolchain.JavaToolchainService
import org.gradle.kotlin.dsl.DependencyHandlerScope
import org.gradle.kotlin.dsl.exclude
import org.gradle.kotlin.dsl.extra
import org.gradle.kotlin.dsl.getByType
import org.gradle.kotlin.dsl.module
import org.gradle.kotlin.dsl.project
import org.gradle.kotlin.dsl.provideDelegate
import org.gradle.kotlin.dsl.withType
import org.gradle.process.JavaForkOptions

/**
 * Apply the given `sparkVersion` as a `strictly` version constraint and [withSparkExcludes] on the
 * current [Dependency].
 */
fun ModuleDependency.forSpark(sparkVersion: String): ModuleDependency {
  val dep = this as ExternalModuleDependency
  dep.version { strictly(sparkVersion) }
  return this.withSparkExcludes()
}

/** Apply a bunch of common dependency-exclusion to the current Spark [Dependency]. */
fun ModuleDependency.withSparkExcludes(): ModuleDependency {
  return this.exclude("commons-logging", "commons-logging")
    .exclude("log4j", "log4j")
    .exclude("org.slf4j", "slf4j-log4j12")
    .exclude("org.slf4j", "slf4j-reload4j")
    .exclude("org.eclipse.jetty", "jetty-util")
    .exclude("org.apache.avro", "avro")
    .exclude("org.apache.arrow", "arrow-vector")
}

fun DependencyHandlerScope.forScala(scalaVersion: String) {
  // Note: Quarkus contains Scala dependencies since 2.9.0
  add("implementation", "org.scala-lang:scala-library:$scalaVersion!!")
  add("implementation", "org.scala-lang:scala-reflect:$scalaVersion!!")
}

/** Forces all [Test] tasks to use the given Java version. */
fun Project.forceJavaVersionForTests(requestedJavaVersion: Int) {
  tasks.withType(Test::class.java).configureEach {
    val currentJavaVersion = JavaVersion.current().majorVersion.toInt()
    if (requestedJavaVersion != currentJavaVersion) {
      useJavaVersion(requestedJavaVersion)
    }
    if (requestedJavaVersion >= 11) {
      addSparkJvmOptions()
    }
  }
}

fun Project.forceJavaVersionForTestTask(name: String, requestedJavaVersion: Int) {
  tasks.named(name, Test::class.java).configure { forceJavaVersion(requestedJavaVersion) }
}

fun Test.forceJavaVersion(requestedJavaVersion: Int) {
  val currentJavaVersion = JavaVersion.current().majorVersion.toInt()
  if (requestedJavaVersion != currentJavaVersion) {
    useJavaVersion(requestedJavaVersion)
  }
  if (requestedJavaVersion >= 11) {
    addSparkJvmOptions()
  }
}

fun JavaExec.forceJavaVersion(requestedJavaVersion: Int) {
  val currentJavaVersion = JavaVersion.current().majorVersion.toInt()
  if (requestedJavaVersion != currentJavaVersion) {
    useJavaVersion(requestedJavaVersion)
  }
  if (requestedJavaVersion >= 11) {
    addSparkJvmOptions()
  }
}

/**
 * Adds the JPMS options required for Spark to run on Java 17, taken from the
 * `DEFAULT_MODULE_OPTIONS` constant in `org.apache.spark.launcher.JavaModuleOptions`.
 */
fun JavaForkOptions.addSparkJvmOptions() {
  jvmArgs =
    (jvmArgs ?: emptyList()) +
      listOf(
        // Spark 3.3+
        "-XX:+IgnoreUnrecognizedVMOptions",
        "--add-opens=java.base/java.lang=ALL-UNNAMED",
        "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
        "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
        "--add-opens=java.base/java.io=ALL-UNNAMED",
        "--add-opens=java.base/java.net=ALL-UNNAMED",
        "--add-opens=java.base/java.nio=ALL-UNNAMED",
        "--add-opens=java.base/java.util=ALL-UNNAMED",
        "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
        "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
        "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
        "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
        "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
        "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED",
        "--add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED",
        // Spark 3.4+
        "-Djdk.reflect.useDirectMethodHandle=false"
      )
}

fun Test.useJavaVersion(requestedJavaVersion: Int) {
  val javaToolchains = project.extensions.findByType(JavaToolchainService::class.java)
  logger.info("Configuring Java $requestedJavaVersion for $path test execution")
  javaLauncher.set(
    javaToolchains!!.launcherFor {
      languageVersion.set(JavaLanguageVersion.of(requestedJavaVersion))
    }
  )
}

fun JavaExec.useJavaVersion(requestedJavaVersion: Int) {
  val javaToolchains = project.extensions.findByType(JavaToolchainService::class.java)
  logger.info("Configuring Java $requestedJavaVersion for $path test execution")
  javaLauncher.set(
    javaToolchains!!.launcherFor {
      languageVersion.set(JavaLanguageVersion.of(requestedJavaVersion))
    }
  )
}

fun Project.libsRequiredVersion(name: String): String {
  val libVer =
    extensions.getByType<VersionCatalogsExtension>().named("libs").findVersion(name).get()
  val reqVer = libVer.requiredVersion
  check(reqVer.isNotEmpty()) {
    "libs-version for '$name' is empty, but must not be empty, version. strict: ${libVer.strictVersion}, required: ${libVer.requiredVersion}, preferred: ${libVer.preferredVersion}"
  }
  return reqVer
}

fun testLogLevel(): String = System.getProperty("test.log.level", "WARN")

fun testLogLevel(minVerbose: String): String {
  val requested = LogLevel.valueOf(testLogLevel().uppercase())
  val minimum = LogLevel.valueOf(minVerbose.uppercase())
  if (requested.ordinal > minimum.ordinal) {
    return minimum.name
  }
  return requested.name
}

fun isIncludedInNesQuEIT(gradle: Gradle): Boolean = "NesQuEIT" == gradle.parent?.rootProject?.name

/** Check whether the current build is run in the context of integrations-testing. */
fun Project.isIncludedInNesQuEIT(): Boolean = isIncludedInNesQuEIT(gradle)

/**
 * Adds an `implementation` dependency to `nessie-client` using the Nessie version supported by the
 * latest released Iceberg version (`versionClientNessie`), if the system property
 * `nessieIntegrationsTesting` is set to `true`.
 */
fun Project.nessieClientForIceberg(): Dependency {
  val dependencyHandlerScope = DependencyHandlerScope.of(dependencies)
  return if (!isIncludedInNesQuEIT()) {
    val clientVersion = libsRequiredVersion("nessieClientVersion")
    dependencies.create(
      if (clientVersion >= "0.50.1") "org.projectnessie.nessie:nessie-client:$clientVersion"
      else "org.projectnessie:nessie-client:$clientVersion"
    )
  } else {
    dependencyHandlerScope.nessieProject("nessie-client")
  }
}

/**
 * Resolves the Nessie Quarkus server for integration tests that depend on it.
 *
 * This is necessary for tools-integrations-testing, because all Nessie projects that depend on
 * Apache Iceberg are handled in a separate build. See `README.md` in the `iceberg/` directory.
 */
fun DependencyHandlerScope.nessieQuarkusServerRunner(): ModuleDependency {
  return nessieProject("nessie-quarkus", "quarkusRunner")
}

/**
 * Resolves a Nessie project in the "right" Gradle build.
 *
 * This is necessary for tools-integrations-testing, because all Nessie projects that depend on
 * Apache Iceberg are handled in a separate build. See `README.md` in the `iceberg/` directory.
 */
fun DependencyHandlerScope.nessieProject(
  artifactId: String,
  configuration: String? = null
): ModuleDependency {
  return if (!isIncludedInNesQuEIT(project(":").dependencyProject.gradle)) {
    project(":$artifactId", configuration)
  } else {
    module(NessieProjects.groupIdForArtifact(artifactId), artifactId, configuration = configuration)
  }
}

fun loadNessieProjects(root: Project) {
  NessieProjects.load(root.resources.text.fromFile("gradle/projects.iceberg.properties"))
}

private class NessieProjects {
  companion object {
    fun groupIdForArtifact(artifactId: String): String {
      return if (integrationsProjects.contains(artifactId)) "org.projectnessie.nessie-integrations"
      else "org.projectnessie.nessie"
    }

    private var integrationsProjects: Set<String> = emptySet()

    fun load(icebergProjects: TextResource) {
      icebergProjects.asReader().use {
        val props = Properties()
        props.load(it)
        integrationsProjects = props.keys.map { k -> k.toString() }.toSet()
      }
    }
  }
}

/** Utility method to check whether a Quarkus build shall produce the uber-jar. */
fun Project.quarkusFatJar(): Boolean = hasProperty("uber-jar") || isIncludedInNesQuEIT()

fun Project.quarkusPackageType(): String = if (quarkusFatJar()) "uber-jar" else "fast-jar"

/** Just load [Properties] from a [File]. */
fun loadProperties(file: File): Properties {
  val props = Properties()
  file.reader().use { reader -> props.load(reader) }
  return props
}

/** Hack for Jandex-Plugin (removed later). */
fun Project.useBuildSubDirectory(buildSubDir: String) {
  project.layout.buildDirectory.set(layout.buildDirectory.dir(buildSubDir).get())

  // TODO open an issue for the Jandex plugin - it configures the task's output directory too
  //  early, so re-assigning the output directory (project.buildDir=...) to a different path
  //  isn't reflected in the Jandex output.
  tasks.withType<JandexProcessResources>().configureEach {
    val sourceSets: SourceSetContainer by project
    sourceSets.all { destinationDir = this.output.resourcesDir!! }
  }
}

/** Resolves the Spark and Scala major versions for all `nessie-spark-extensions*` projects. */
fun Project.getSparkScalaVersionsForProject(): SparkScalaVersions {
  val sparkScala = project.name.split("-").last().split("_")

  val sparkMajorVersion = if (sparkScala[0][0].isDigit()) sparkScala[0] else "3.2"
  val scalaMajorVersion = sparkScala[1]

  useBuildSubDirectory(scalaMajorVersion)

  return useSparkScalaVersionsForProject(sparkMajorVersion, scalaMajorVersion)
}

fun Project.scalaDependencyVersion(scalaMajorVersion: String): String {
  val versionCatalog = extensions.getByType<VersionCatalogsExtension>().named("libs")
  val scalaDepName = "scala-library-v${scalaMajorVersion.replace("[.]".toRegex(), "")}"
  val scalaDep =
    versionCatalog.findLibrary(scalaDepName).orElseThrow {
      IllegalStateException("No library '$scalaDepName' defined in version catalog 'libs'")
    }
  return scalaDep.get().versionConstraint.preferredVersion
}

fun Project.sparkDependencyVersion(sparkMajorVersion: String, scalaMajorVersion: String): String {
  val versionCatalog = extensions.getByType<VersionCatalogsExtension>().named("libs")
  val sparkDepName =
    "spark-sql-v${sparkMajorVersion.replace("[.]".toRegex(), "")}-v${scalaMajorVersion.replace("[.]".toRegex(), "")}"
  val sparkDep =
    versionCatalog.findLibrary(sparkDepName).orElseThrow {
      IllegalStateException("No library '$sparkDepName' defined in version catalog 'libs'")
    }
  return sparkDep.get().versionConstraint.preferredVersion
}

fun Project.useSparkScalaVersionsForProject(sparkMajorVersion: String): SparkScalaVersions {
  val scalaMajorVersion =
    rootProject.extra["sparkVersion-${sparkMajorVersion}-scalaVersions"]
      .toString()
      .split(",")
      .map { it.trim() }[0]
  return useSparkScalaVersionsForProject(sparkMajorVersion, scalaMajorVersion)
}

fun Project.useSparkScalaVersionsForProject(
  sparkMajorVersion: String,
  scalaMajorVersion: String
): SparkScalaVersions {
  return SparkScalaVersions(
    sparkMajorVersion,
    scalaMajorVersion,
    sparkDependencyVersion(sparkMajorVersion, scalaMajorVersion),
    scalaDependencyVersion(scalaMajorVersion),
    javaVersionForSpark(sparkMajorVersion)
  )
}

/**
 * Get the newest Java LTS version that is lower than or equal to the currently running Java
 * version.
 *
 * For Spark 3.2, this is always Java 11. For Spark 3.3 and 3.4, this is Java 17 when running the
 * build on Java 17 or newer, otherwise Java 11.
 */
fun javaVersionForSpark(sparkMajorVersion: String): Int {
  val currentJavaVersion = JavaVersion.current().majorVersion.toInt()
  return when (sparkMajorVersion) {
    "3.2" -> 11
    "3.3",
    "3.4",
    "3.5" ->
      when {
        currentJavaVersion >= 17 -> 17
        else -> 11
      }
    else ->
      throw IllegalArgumentException(
        "Do not know which Java version Spark $sparkMajorVersion supports"
      )
  }
}

class SparkScalaVersions(
  val sparkMajorVersion: String,
  val scalaMajorVersion: String,
  val sparkVersion: String,
  val scalaVersion: String,
  val runtimeJavaVersion: Int
)

abstract class UnixExecutableTask : DefaultTask() {
  @get:OutputFile abstract val executable: RegularFileProperty

  @get:InputFile
  @get:PathSensitive(PathSensitivity.RELATIVE)
  abstract val template: RegularFileProperty

  @get:InputFile
  @get:PathSensitive(PathSensitivity.RELATIVE)
  abstract val sourceJar: RegularFileProperty

  @TaskAction
  fun exec() {
    val exec = executable.get().asFile
    exec.parentFile.mkdirs()
    exec.outputStream().use { out ->
      template.get().asFile.inputStream().use { i -> i.transferTo(out) }
      sourceJar.get().asFile.inputStream().use { i -> i.transferTo(out) }
    }
    exec.setExecutable(true)
  }
}

class ReplaceInFiles(val files: FileTree, val replacements: Map<String, String>) : Action<Task> {
  override fun execute(task: Task) {
    files.forEach { f ->
      val src = f.readText()
      var txt = src
      for (e in replacements.entries) {
        txt = txt.replace(e.key, e.value)
      }
      if (txt != src) {
        f.writeText(txt)
      }
    }
  }
}

class WriteFile(val file: File, val text: String) : Action<Task> {
  override fun execute(task: Task) {
    file.writeText(text)
  }
}

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

if (!JavaVersion.current().isCompatibleWith(JavaVersion.VERSION_11)) {
  throw GradleException("Build requires Java 11")
}

val baseVersion = file("version.txt").readText().trim()

pluginManagement {
  // Cannot use a settings-script global variable/value, so pass the 'versions' Properties via
  // settings.extra around.
  val versions = java.util.Properties()
  val pluginIdPattern =
    java.util.regex.Pattern.compile("\\s*id\\(\"([^\"]+)\"\\) version \"([^\"]+)\"\\s*")
  settings.extra["nessieBuild.versions"] = versions

  repositories {
    mavenCentral() // prefer Maven Central, in case Gradle's repo has issues
    gradlePluginPortal()
    if (System.getProperty("withMavenLocal").toBoolean()) {
      mavenLocal()
    }
  }

  plugins {

    // Note: this is NOT a real project but a hack for dependabot to manage the plugin versions.
    //
    // Background: dependabot only manages dependencies (incl Gradle plugins) in build.gradle[.kts]
    // files. It scans the root build.gradle[.kts] fila and those in submodules referenced in
    // settings.gradle[.kts].
    // But dependabot does not manage managed plugin dependencies in settings.gradle[.kts].
    // However, since dependabot is a "dumb search and replace engine", we can use a trick:
    // 1. Have this "dummy" build.gradle.kts file with all managed plugin dependencies.
    // 2. Add an `include()` to this build file in settings.gradle.kts, surrounded with an
    //    `if (false)`, so Gradle does _not_ pick it up.
    // 3. Parse this file in our settings.gradle.kts, provide a `ResolutionStrategy` to the
    //    plugin dependencies.

    val pulledVersions =
      file("gradle/dependabot/build.gradle.kts")
        .readLines()
        .map { line -> pluginIdPattern.matcher(line) }
        .filter { matcher -> matcher.matches() }
        .associate { matcher -> matcher.group(1) to matcher.group(2) }

    resolutionStrategy {
      eachPlugin {
        if (requested.version == null) {
          var pluginId = requested.id.id
          if (
            pluginId.startsWith("org.projectnessie.buildsupport.") ||
              pluginId == "org.projectnessie.smallrye-open-api"
          ) {
            pluginId = "org.projectnessie.buildsupport.spotless"
          }
          if (pulledVersions.containsKey(pluginId)) {
            useVersion(pulledVersions[pluginId])
          }
        }
      }
    }

    versions["versionQuarkus"] = pulledVersions["io.quarkus"]
    versions["versionErrorPronePlugin"] = pulledVersions["net.ltgt.errorprone"]
    versions["versionIdeaExtPlugin"] = pulledVersions["org.jetbrains.gradle.plugin.idea-ext"]
    versions["versionSpotlessPlugin"] = pulledVersions["com.diffplug.spotless"]
    versions["versionJandexPlugin"] = pulledVersions["com.github.vlsi.jandex"]
    versions["versionShadowPlugin"] = pulledVersions["com.github.johnrengelman.plugin-shadow"]
    versions["versionNessieBuildPlugins"] =
      pulledVersions["org.projectnessie.buildsupport.spotless"]

    // The project's settings.gradle.kts is "executed" before buildSrc's settings.gradle.kts and
    // build.gradle.kts.
    //
    // Plugin and important dependency versions are defined here and shared with buildSrc via
    // a properties file, and via an 'extra' property with all other modules of the Nessie build.
    //
    // This approach works fine with GitHub's dependabot as well
    val nessieBuildVersionsFile = file("build/nessieBuild/versions.properties")
    nessieBuildVersionsFile.parentFile.mkdirs()
    nessieBuildVersionsFile.outputStream().use {
      versions.store(it, "Nessie Build versions from settings.gradle.kts - DO NOT MODIFY!")
    }
  }
}

gradle.rootProject {
  val prj = this
  val versions = settings.extra["nessieBuild.versions"] as java.util.Properties
  versions.forEach { k, v -> prj.extra[k.toString()] = v }
}

gradle.beforeProject {
  version = baseVersion
  group = "org.projectnessie"
}

include("code-coverage")

fun nessieProject(name: String, directory: String) {
  include(name)
  project(":$name").projectDir = file(directory)
}

nessieProject("nessie-bom", "bom")

nessieProject("nessie-clients", "clients")

nessieProject("nessie-spark-antlr-runtime", "clients/antlr-runtime")

nessieProject("nessie-client", "clients/client")

nessieProject("nessie-compatibility", "compatibility")

nessieProject("nessie-compatibility-common", "compatibility/common")

nessieProject("nessie-compatibility-tests", "compatibility/compatibility-tests")

nessieProject("nessie-compatibility-jersey", "compatibility/jersey")

nessieProject("nessie-gc", "gc")

nessieProject("nessie-model", "model")

nessieProject("nessie-perftest", "perftest")

nessieProject("nessie-perftest-gatling", "perftest/gatling")

nessieProject("nessie-perftest-simulations", "perftest/simulations")

nessieProject("nessie-server-parent", "servers")

nessieProject("nessie-servers-iceberg-fixtures", "servers/iceberg-fixtures")

nessieProject("nessie-jaxrs", "servers/jax-rs")

nessieProject("nessie-jaxrs-testextension", "servers/jax-rs-testextension")

nessieProject("nessie-jaxrs-tests", "servers/jax-rs-tests")

nessieProject("nessie-lambda", "servers/lambda")

nessieProject("nessie-quarkus-cli", "servers/quarkus-cli")

nessieProject("nessie-quarkus-common", "servers/quarkus-common")

nessieProject("nessie-quarkus", "servers/quarkus-server")

nessieProject("nessie-quarkus-tests", "servers/quarkus-tests")

nessieProject("nessie-rest-services", "servers/rest-services")

nessieProject("nessie-services", "servers/services")

nessieProject("nessie-server-store", "servers/store")

nessieProject("nessie-server-store-proto", "servers/store-proto")

nessieProject("nessie-tools", "tools")

nessieProject("nessie-content-generator", "tools/content-generator")

nessieProject("nessie-ui", "ui")

nessieProject("nessie-versioned", "versioned")

nessieProject("nessie-versioned-persist", "versioned/persist")

nessieProject("nessie-versioned-persist-adapter", "versioned/persist/adapter")

nessieProject("nessie-versioned-persist-bench", "versioned/persist/bench")

nessieProject("nessie-versioned-persist-dynamodb", "versioned/persist/dynamodb")

nessieProject("nessie-versioned-persist-in-memory", "versioned/persist/inmem")

nessieProject("nessie-versioned-persist-mongodb", "versioned/persist/mongodb")

nessieProject("nessie-versioned-persist-non-transactional", "versioned/persist/nontx")

nessieProject("nessie-versioned-persist-rocks", "versioned/persist/rocks")

nessieProject("nessie-versioned-persist-serialize", "versioned/persist/serialize")

nessieProject("nessie-versioned-persist-serialize-proto", "versioned/persist/serialize-proto")

nessieProject("nessie-versioned-persist-store", "versioned/persist/store")

nessieProject("nessie-versioned-persist-tests", "versioned/persist/tests")

nessieProject("nessie-versioned-persist-transactional", "versioned/persist/tx")

nessieProject("nessie-versioned-spi", "versioned/spi")

nessieProject("nessie-versioned-tests", "versioned/tests")

// Needed when loading/syncing the whole integrations-tools-testing project with Nessie as an
// included build. IDEA gets here two times: the first run _does_ have the properties from the
// integrations-tools-testing build's `gradle.properties` file, while the 2nd invocation only runs
// from the included build.
if (gradle.parent != null && System.getProperty("idea.sync.active").toBoolean()) {
  val additionalPropertiesFile = file("./build/additional-build.properties")
  if (additionalPropertiesFile.isFile) {
    val additionalProperties = java.util.Properties()
    additionalPropertiesFile.reader().use { reader -> additionalProperties.load(reader) }
    System.getProperties().putAll(additionalProperties)
  }
}

// Cannot use isIntegrationsTestingEnabled() in buildSrc/src/main/kotlin/Utilities.kt, because
// settings.gradle is evaluated before buildSrc.
if (!System.getProperty("nessie.integrationsTesting.enable").toBoolean()) {
  nessieProject("nessie-deltalake", "clients/deltalake")

  nessieProject("iceberg-views", "clients/iceberg-views")

  nessieProject("nessie-spark-3.2-extensions", "clients/spark-3.2-extensions")

  nessieProject("nessie-spark-extensions-grammar", "clients/spark-antlr-grammar")

  nessieProject("nessie-spark-extensions", "clients/spark-extensions")

  nessieProject("nessie-spark-extensions-base", "clients/spark-extensions-base")

  nessieProject("nessie-gc-base", "gc/gc-base")
}

if (false) {
  include("gradle:dependabot")
}

rootProject.name = "nessie"

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

import com.diffplug.gradle.spotless.SpotlessTask
import com.github.gradle.node.npm.task.NpmInstallTask
import com.github.gradle.node.npm.task.NpmSetupTask
import com.github.gradle.node.npm.task.NpmTask
import com.github.gradle.node.task.NodeSetupTask

plugins {
  // The ':nessie-ui' module is technically not a Java library, but declaring it as such provides
  // all
  // the "essential" side effects: pre-configured "basic" tasks (clean, test, check, jar) plus
  // Maven publications.
  `java-library`
  `maven-publish`
  signing
  alias(libs.plugins.node.gradle)
  `nessie-conventions`
}

node {
  download.set(true)
  version.set("16.14.2")
  npmVersion.set("7.24.2")
}

val dotGradle = project.projectDir.resolve(".gradle")
val npmBuildTarget = project.buildDir.resolve("npm")
val npmBuildDir = npmBuildTarget.resolve("META-INF/resources")
val openApiSpecDir = project.projectDir.resolve("src/openapi")
val generatedOpenApiCodeUnfixed = project.buildDir.resolve("generated/ts")
val generatedOpenApiCode = project.projectDir.resolve("src/generated")
val testCoverageDir = project.projectDir.resolve("coverage")
val nodeModulesDir = project.projectDir.resolve("node_modules")
val shadowPackageJson = project.buildDir.resolve("packageJson")

val clean =
  tasks.named("clean") {
    doFirst {
      delete(dotGradle)
      delete(nodeModulesDir)
      delete(project.projectDir.resolve("tsconfig.tsbuildinfo"))
      delete(testCoverageDir)
      delete(generatedOpenApiCode)
    }
  }

val nodeSetup =
  tasks.named<NodeSetupTask>("nodeSetup") {
    mustRunAfter(clean)
    doFirst { delete(dotGradle.resolve("nodejs")) }
  }

val npmSetup =
  tasks.named<NpmSetupTask>("npmSetup") {
    mustRunAfter(clean, nodeSetup)
    doFirst { delete(dotGradle.resolve("npm")) }
  }

val npmInstallReal =
  tasks.named<NpmInstallTask>("npmInstall") {
    doFirst {
      // Delete the node_modules dir so it stays consistent
      delete(nodeModulesDir)
    }
  }

// The node_modules directory is huge (> 500M), so checking the contents takes a couple of seconds.
// To mitigate the effect, this script uses a trick by using the package*.json files and only
// execute
// the "real" npmInstall, when those are our of sync.
val npmInstall =
  tasks.register("npmInstallFacade") {
    mustRunAfter(clean)
    dependsOn(npmSetup)
    outputs.cacheIf {
      project.buildDir
        .resolve("npm/npm-v${node.npmVersion}/lib/node_modules/npm/node_modules")
        .isDirectory()
    }
    // Need to add the fully qualified path here, so a "cached" npmInstallFacade run from another
    // directory with Nessie doesn't let "this" code-tree "think" that it has one.
    inputs.property("node.modules.dir", project.projectDir)
    inputs.property("node.version", node.version)
    inputs.property("npm.version", node.npmVersion)
    inputs.files("package.json", "package-lock.json").withPathSensitivity(PathSensitivity.RELATIVE)
    outputs.dir(shadowPackageJson)
    doFirst {
      sync {
        from(".")
        include("package*.json")
        into(shadowPackageJson)
      }
      npmInstallReal.get().exec()
    }
  }

val npmGenerateAPI =
  tasks.register<NpmTask>("npmGenerateApi") {
    description = "Generate from OpenAPI spec"
    dependsOn(npmInstall)
    inputs.dir(openApiSpecDir).withPathSensitivity(PathSensitivity.RELATIVE)
    outputs.dir(generatedOpenApiCodeUnfixed)
    doFirst {
      // Remove previously generated code to have generated files consistent with the OpenAPI spec
      delete(generatedOpenApiCodeUnfixed)
    }
    args.set(listOf("run", "generate-api"))
  }

val npmFixGeneratedClient =
  tasks.register<NpmTask>("npmFixGeneratedClient") {
    description = "Fix generated OpenAPI code"
    dependsOn(npmGenerateAPI)
    inputs.dir(generatedOpenApiCodeUnfixed).withPathSensitivity(PathSensitivity.RELATIVE)
    outputs.dir(generatedOpenApiCode)
    doFirst {
      sync {
        // "Sync" ensures that the contents in src/generated match the contents of the
        // code generated via npmGenerateAPI
        into(generatedOpenApiCode)
        from(generatedOpenApiCodeUnfixed.resolve("src/generated"))
      }
    }
    args.set(listOf("run", "fix-generated-client"))
  }

/*
   <!-- sourcemap generation takes 11 seconds or more and isn't necessary for production -->
   <ui.generate-sourcemap>false</ui.generate-sourcemap>
   <!-- ES Lint takes 17 seconds or more, but should be enabled for CI -->
   <ui.disable-es-lint>false</ui.disable-es-lint>
   <!-- Terser takes 5 seconds or more, but should be enabled for CI -->
   <ui.disable-terser>false</ui.disable-terser>
   <!-- requires manual installation of `speed-measure-webpack-plugin` -->
   <ui.profile-plugins>false</ui.profile-plugins>
*/

val npmBuild =
  tasks.register<NpmTask>("npmBuild") {
    description = "Run 'npm build'"
    dependsOn(npmFixGeneratedClient)
    inputs.dir(project.projectDir.resolve("config")).withPathSensitivity(PathSensitivity.RELATIVE)
    inputs.dir(project.projectDir.resolve("public")).withPathSensitivity(PathSensitivity.RELATIVE)
    inputs.dir(project.projectDir.resolve("scripts")).withPathSensitivity(PathSensitivity.RELATIVE)
    inputs.dir(project.projectDir.resolve("src")).withPathSensitivity(PathSensitivity.RELATIVE)
    inputs.dir(generatedOpenApiCode).withPathSensitivity(PathSensitivity.RELATIVE)
    inputs.files("*.json", "*.js").withPathSensitivity(PathSensitivity.RELATIVE)
    outputs.dir(npmBuildDir)
    doFirst {
      // Remove all previously generated output
      delete(npmBuildTarget)
    }
    args.set(listOf("run", "build"))
    environment.put("GENERATE_SOURCEMAP", "false")
    environment.put("DISABLE_ESLINT_PLUGIN", "false")
    environment.put("DISABLE_TERSER_PLUGIN", "false")
    environment.put("PROFILE_PLUGINS", "false")
  }

val npmTest =
  tasks.register<NpmTask>("npmTest") {
    description = "NPM test"
    dependsOn(npmBuild)
    inputs.dir(npmBuildDir).withPathSensitivity(PathSensitivity.RELATIVE)
    outputs.dir(testCoverageDir)
    doFirst { delete(testCoverageDir) }
    npmCommand.add("test")
    args.addAll("--", "--coverage")
  }

val npmLint =
  tasks.register<NpmTask>("npmLint") {
    description = "NPM lint"
    dependsOn(npmBuild)
    inputs.dir(project.projectDir.resolve("src")).withPathSensitivity(PathSensitivity.RELATIVE)
    inputs.files("*.json", "*.js").withPathSensitivity(PathSensitivity.RELATIVE)
    outputs.file(project.buildDir.resolve("npmLintRun"))
    doLast { project.buildDir.resolve("npmLintRun").writeText("linted") }
    args.set(listOf("run", "lint"))
  }

tasks.withType<NpmTask>().configureEach {
  inputs.property("node.version", node.version)
  inputs.property("npm.version", node.npmVersion)
  inputs.files("package.json", "package-lock.json").withPathSensitivity(PathSensitivity.RELATIVE)
  outputs.cacheIf { true }
  environment.put("CI", "true")
  environment.put("BUILD_PATH", npmBuildDir.path)
  val javaDir =
    javaToolchains
      .launcherFor { languageVersion.set(JavaLanguageVersion.of(11)) }
      .get()
      .metadata
      .installationPath
  environment.put(
    "PATH",
    "${System.getenv("PATH")}${System.getProperty("path.separator")}$javaDir/bin"
  )
}

tasks.named<Jar>("jar") { dependsOn(npmBuild) }

// This is a hack to let nessie-quarkus-server unit tests discover the static web resources.
// Tests run via the `test` task use "reloadable" sources - aka source directories, not built jar -
// to serve static web resources. The Quarkus test machinery scans for resource folders of the
// dependent projects - so we add the directory created during the ui-jar build as a resource
// folder.
// Hack-ish, but works.
sourceSets { main { resources { srcDir(npmBuildTarget) } } }

tasks.named("processResources") { mustRunAfter(npmBuild, npmFixGeneratedClient) }

tasks.named<Jar>("sourcesJar") {
  mustRunAfter(npmBuild)
  dependsOn(npmFixGeneratedClient)
  from("src")
}

tasks.withType<SpotlessTask>().configureEach {
  // Declare this unnecessary dependency here, otherwise Gradle complains about spotlessXml using
  // the output of npmFixGeneratedClient, which disables build optimizations
  mustRunAfter(npmFixGeneratedClient)
}

tasks.named("test") { dependsOn(npmTest) }

tasks.named("check") {
  dependsOn(npmTest)
  dependsOn(npmLint)
}

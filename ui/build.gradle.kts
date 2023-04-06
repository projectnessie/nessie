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
import com.github.gradle.node.npm.task.NpmSetupTask
import com.github.gradle.node.npm.task.NpmTask
import com.github.gradle.node.task.NodeSetupTask
import org.apache.tools.ant.taskdefs.condition.Os
import org.gradle.api.internal.file.FileOperations

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

val dotGradle = rootProject.rootDir.resolve(".gradle")

node {
  download.set(true)
  version.set("18.15.0")
  npmVersion.set("9.5.0")
  workDir.set(dotGradle.resolve("nodejs"))
  npmWorkDir.set(dotGradle.resolve("npm"))
  yarnWorkDir.set(dotGradle.resolve("yarn"))
  pnpmWorkDir.set(dotGradle.resolve("pnpm"))
}

val npmBuildTarget = project.buildDir.resolve("npm")
val npmBuildDir = npmBuildTarget.resolve("META-INF/resources")
val openApiSpecDir = project.projectDir.resolve("src/openapi")
val generatedOpenApiCode = project.projectDir.resolve("src/generated")
val testCoverageDir = project.projectDir.resolve("coverage")
val nodeModulesDir = project.projectDir.resolve("node_modules")
val shadowPackageJson = project.buildDir.resolve("packageJson")

val fs = properties.get("fileOperations") as FileOperations

class DeleteFiles(private val fs: FileOperations, private val files: Any) : Action<Task> {
  override fun execute(task: Task) {
    fs.delete(files)
  }
}

val clean =
  tasks.named("clean") {
    doFirst(DeleteFiles(fs, nodeModulesDir))
    doFirst(DeleteFiles(fs, project.projectDir.resolve("tsconfig.tsbuildinfo")))
    doFirst(DeleteFiles(fs, testCoverageDir))
    doFirst(DeleteFiles(fs, generatedOpenApiCode))
  }

val nodeSetup =
  tasks.named<NodeSetupTask>("nodeSetup") {
    mustRunAfter(clean)
    logging.captureStandardOutput(LogLevel.INFO)
    logging.captureStandardError(LogLevel.LIFECYCLE)
  }

val npmSetup =
  tasks.named<NpmSetupTask>("npmSetup") {
    mustRunAfter(clean, nodeSetup)
    logging.captureStandardOutput(LogLevel.INFO)
    logging.captureStandardError(LogLevel.LIFECYCLE)
  }

class NpmInstallCacheIf(val dotGradleDir: File, val npmVersion: Property<String>) : Spec<Task> {
  override fun isSatisfiedBy(element: Task?): Boolean {
    // Do not let the UI module assume that npm is already setup (#4461)
    //
    // ... which can happen when using multiple Git worktrees, when one of those
    // did the NPM-setup dance, letting the "other" Git worktree "think", that
    // it did already the NPM-setup dance, too.
    return dotGradleDir
      .resolve("npm/npm-v${npmVersion}/lib/node_modules/npm/node_modules")
      .isDirectory
  }
}

class NpmInstallActionSync(private val target: File) : Action<SyncSpec> {
  override fun execute(sync: SyncSpec) {
    sync.from(".")
    sync.include("package*.json")
    sync.into(target)
  }
}

class NpmInstallSync(private val fileOperations: FileOperations, private val target: File) :
  Action<Task> {
  override fun execute(t: Task) {
    fileOperations.sync(NpmInstallActionSync(target))
  }
}

// The node_modules directory is huge (> 500M), so checking the contents takes a couple of seconds.
// To mitigate the effect, this script uses a trick by using the package*.json files and only
// execute
// the "real" npmInstall, when those are our of sync.
val npmInstall =
  tasks.named("npmInstall") {
    mustRunAfter(clean)
    dependsOn(npmSetup)
    outputs.cacheIf(NpmInstallCacheIf(dotGradle, node.npmVersion))
    // Need to add the fully qualified path here, so a "cached" npmInstallFacade run from another
    // directory with Nessie doesn't let "this" code-tree "think" that it has one.
    inputs.property("node.modules.dir", project.projectDir)
    inputs.property("node.version", node.version)
    inputs.property("npm.version", node.npmVersion)
    inputs.files("package.json", "package-lock.json").withPathSensitivity(PathSensitivity.RELATIVE)
    outputs.dir(shadowPackageJson)
    logging.captureStandardOutput(LogLevel.INFO)
    logging.captureStandardError(LogLevel.LIFECYCLE)
    // Delete the node_modules dir so it stays consistent
    doFirst(NpmInstallSync(fs, shadowPackageJson))
    doFirst(DeleteFiles(fs, nodeModulesDir))
  }

val openapiGenerator by configurations.creating

dependencies { openapiGenerator(libs.openapi.generator.cli) }

val npmGenerateAPI =
  tasks.register<JavaExec>("npmGenerateApi") {
    description = "Generate from OpenAPI spec"
    outputs.cacheIf { true }
    inputs.dir(openApiSpecDir).withPathSensitivity(PathSensitivity.RELATIVE)
    outputs.dir(generatedOpenApiCode)
    classpath(openapiGenerator)
    mainClass.set("org.openapitools.codegen.OpenAPIGenerator")
    logging.captureStandardOutput(LogLevel.INFO)
    logging.captureStandardError(LogLevel.LIFECYCLE)
    args(
      "generate",
      "-g",
      "typescript-fetch",
      "-i",
      "${openApiSpecDir.relativeTo(projectDir)}/nessie-openapi-0.45.0.yaml",
      "-o",
      "src/generated/utils/api",
      "--additional-properties=supportsES6=true"
    )
    // Remove previously generated code to have generated files consistent with the OpenAPI spec
    doFirst(DeleteFiles(fs, generatedOpenApiCode))
    // openapi-generator produces Line 264 in runtime.ts as
    //    export type FetchAPI = GlobalFetch['fetch'];
    // but must be
    //    export type FetchAPI = WindowOrWorkerGlobalScope['fetch'];
    doLast(
      ReplaceInFiles(
        fileTree(generatedOpenApiCode.resolve("utils/api/runtime.ts")),
        mapOf("GlobalFetch" to "WindowOrWorkerGlobalScope")
      )
    )
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
    dependsOn(npmInstall, npmGenerateAPI)
    inputs.dir(project.projectDir.resolve("config")).withPathSensitivity(PathSensitivity.RELATIVE)
    inputs.dir(project.projectDir.resolve("public")).withPathSensitivity(PathSensitivity.RELATIVE)
    inputs.dir(project.projectDir.resolve("scripts")).withPathSensitivity(PathSensitivity.RELATIVE)
    inputs.dir(project.projectDir.resolve("src")).withPathSensitivity(PathSensitivity.RELATIVE)
    inputs.dir(generatedOpenApiCode).withPathSensitivity(PathSensitivity.RELATIVE)
    inputs
      .files(fileTree(".") { include("*.json", "*.js") })
      .withPathSensitivity(PathSensitivity.RELATIVE)
    outputs.dir(npmBuildDir)
    // Remove all previously generated output
    doFirst(DeleteFiles(fs, npmBuildTarget))
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
    inputs.dir(project.projectDir.resolve("config")).withPathSensitivity(PathSensitivity.RELATIVE)
    inputs.dir(project.projectDir.resolve("public")).withPathSensitivity(PathSensitivity.RELATIVE)
    inputs.dir(project.projectDir.resolve("scripts")).withPathSensitivity(PathSensitivity.RELATIVE)
    inputs.dir(project.projectDir.resolve("src")).withPathSensitivity(PathSensitivity.RELATIVE)
    inputs.dir(generatedOpenApiCode).withPathSensitivity(PathSensitivity.RELATIVE)
    inputs.dir(npmBuildDir).withPathSensitivity(PathSensitivity.RELATIVE)
    outputs.dir(testCoverageDir)
    doFirst(DeleteFiles(fs, testCoverageDir))
    npmCommand.add("test")
    args.addAll("--", "--coverage")
    usesService(
      gradle.sharedServices.registrations.named("testParallelismConstraint").get().service
    )
  }

val npmLint =
  tasks.register<NpmTask>("npmLint") {
    description = "NPM lint"
    dependsOn(npmBuild)
    inputs.dir(npmBuildDir).withPathSensitivity(PathSensitivity.RELATIVE)
    inputs.dir(project.projectDir.resolve("src")).withPathSensitivity(PathSensitivity.RELATIVE)
    inputs
      .files(fileTree(".") { include("*.json", "*.js") })
      .withPathSensitivity(PathSensitivity.RELATIVE)
    val lintedMarker = project.buildDir.resolve("npmLintRun")
    outputs.file(lintedMarker)
    doFirst(DeleteFiles(fs, lintedMarker))
    doLast(WriteFile(lintedMarker, "linted"))
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
  logging.captureStandardOutput(LogLevel.INFO)
  logging.captureStandardError(LogLevel.LIFECYCLE)
}

tasks.named<Jar>("jar") {
  outputs.cacheIf {
    // The jar is small, the build time for it is high
    true
  }
  from(npmBuild)
}

// This is a hack to let nessie-quarkus-server unit tests discover the static web resources.
// Tests run via the `test` task use "reloadable" sources - aka source directories, not built jar -
// to serve static web resources. The Quarkus test machinery scans for resource folders of the
// dependent projects - so we add the directory created during the ui-jar build as a resource
// folder.
// Hack-ish, but works.
sourceSets { main { resources { srcDir(npmBuildTarget) } } }

tasks.named("processResources") { mustRunAfter(npmBuild, npmGenerateAPI) }

tasks.named<Jar>("sourcesJar") {
  mustRunAfter(npmBuild)
  from(npmGenerateAPI)
  from("src")
}

tasks.withType<SpotlessTask>().configureEach {
  // Declare this unnecessary dependency here, otherwise Gradle complains about spotlessXml using
  // the output of npmFixGeneratedClient, which disables build optimizations
  mustRunAfter(npmGenerateAPI)
}

tasks.named("test") { dependsOn(npmTest) }

tasks.named("check") {
  dependsOn(tasks.named("test"))
  dependsOn(npmLint)
}

// NPM tests regularly (nearly consistently) fail
if (Os.isFamily(Os.FAMILY_WINDOWS)) {
  tasks.named("test") { this.enabled = false }
}

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

package org.projectnessie.gradle.licenses

import java.io.File
import java.nio.file.Path
import java.util.Properties
import java.util.jar.JarOutputStream
import kotlin.io.path.createDirectories
import kotlin.io.path.outputStream
import kotlin.io.path.readText
import kotlin.io.path.writeText
import org.assertj.core.api.Assertions.assertThat
import org.gradle.testkit.runner.GradleRunner
import org.gradle.testkit.runner.TaskOutcome
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir

class LicenseChecksPluginTest {
  @TempDir lateinit var projectDir: Path

  @Test
  fun policyChangesInvalidateCacheAndKeepAdditiveModuleAllowances() {
    fixture()
    val first = runner().build()
    assertThat(first.task(":licensee")?.outcome).isEqualTo(TaskOutcome.SUCCESS)
    assertThat(first.output).contains("Configuration cache entry stored")
    assertThat(runner().build().output).contains("Configuration cache entry reused")
    val policyFile = projectDir.resolve("policy.json")
    val original = policyFile.readText()
    policyFile.writeText(original.replace("[\"Apache-2.0\"]", "[]"))
    val rejected = runner().buildAndFail()
    assertThat(rejected.output).contains("policy.json", "has changed", "example:apache:1")
    assertThat(rejected.task(":licensee")?.outcome).isEqualTo(TaskOutcome.FAILED)
    assertThat(rejected.output)
      .doesNotContain("example:mit:1", "example:custom:1", "example:missing:1")
    policyFile.writeText(original)
    runner().build()
    assertThat(runner().build().output).contains("Configuration cache entry reused")
  }

  @Test
  fun ignoredOwnGroupsStillCheckTheirExternalTransitiveDependencies() {
    fixture()
    runner().build()
    val report = projectDir.resolve("build/reports/licensee/artifacts.json").readText()
    assertThat(report)
      .contains("apache", "mit", "custom", "missing")
      .doesNotContain("org.projectnessie.nessie")
  }

  @Test
  fun appliesThroughPrivateScriptClasspathWithCacheReuse() {
    fixture(privateBootstrap = true)
    assertThat(runner(privateBootstrap = true).build().output)
      .contains("Configuration cache entry stored")
    assertThat(runner(privateBootstrap = true).build().output)
      .contains("Configuration cache entry reused")
  }

  @Test
  fun rejectsInvalidPolicyDuringConfiguration() {
    fixture()
    projectDir.resolve("policy.json").writeText("""{"allowedLicense": []}""")
    val rejected = runner().buildAndFail()
    assertThat(rejected.output).contains("Unknown license policy field", "policy.json")
    assertThat(rejected.task(":licensee")).isNull()
  }

  private fun fixture(privateBootstrap: Boolean = false) {
    projectDir.resolve("settings.gradle").writeText("rootProject.name = 'license-policy-fixture'\n")
    val application =
      if (privateBootstrap) {
        """
        plugins { id 'java' }
        apply from: 'policy-bootstrap.gradle'
        """
      } else {
        """
        plugins {
          id 'java'
          id 'nessie-licensee-policy'
        }
        licenseChecks { policyFrom(layout.projectDirectory.file('policy.json')) }
        licensee { allow('MIT') }
        """
      }
    projectDir
      .resolve("build.gradle")
      .writeText(
        application.trimIndent() +
          "\n" +
          """
          repositories { maven { url = uri('repo') } }
          dependencies {
            runtimeOnly 'org.projectnessie.nessie:owned:1'
            runtimeOnly 'org.projectnessie.nessie-integrations:owned:1'
            runtimeOnly 'example:custom:1'
            runtimeOnly 'example:missing:1'
          }
          """
            .trimIndent() +
          "\n",
      )
    if (privateBootstrap) {
      val metadata = Properties()
      javaClass.classLoader.getResourceAsStream("plugin-under-test-metadata.properties").use {
        metadata.load(requireNotNull(it))
      }
      val classpath =
        metadata.getProperty("implementation-classpath").split(File.pathSeparator).joinToString(
          ",",
        ) {
          "'" + it.replace("\\", "\\\\").replace("'", "\\'") + "'"
        }
      projectDir
        .resolve("policy-bootstrap.gradle")
        .writeText(
          """
        buildscript { dependencies { classpath files($classpath) } }
        apply plugin: org.projectnessie.gradle.licenses.LicenseChecksPlugin
        extensions.configure(org.projectnessie.gradle.licenses.LicenseChecksExtension) { policy ->
          policy.policyFrom(layout.projectDirectory.file('policy.json'))
        }
        extensions.configure(app.cash.licensee.LicenseeExtension) { policy -> policy.allow('MIT') }
        """
            .trimIndent() + "\n",
        )
    }
    projectDir
      .resolve("policy.json")
      .writeText(
        """
        {
          "allowedLicenses": ["Apache-2.0"],
          "allowedUrls": [{"url":"https://fixtures.invalid/approved","reason":"Fixture approved URL"}],
          "allowedDependencies": [{"group":"example","artifact":"missing","version":"1","reason":"Fixture exact exception"}]
        }
        """
          .trimIndent() + "\n",
      )
    artifact(
      "example",
      "apache",
      "Apache License, Version 2.0",
      "https://www.apache.org/licenses/LICENSE-2.0.txt",
    )
    artifact("example", "mit", "MIT License", "https://opensource.org/licenses/MIT")
    artifact("example", "custom", "Fixture approved license", "https://fixtures.invalid/approved")
    artifact("example", "missing", "MIT License", null)
    artifact(
      "org.projectnessie.nessie",
      "owned",
      "Own project license",
      "https://fixtures.invalid/own",
      "apache",
    )
    artifact(
      "org.projectnessie.nessie-integrations",
      "owned",
      "Own project license",
      "https://fixtures.invalid/own",
      "mit",
    )
  }

  private fun artifact(
    group: String,
    name: String,
    license: String,
    url: String?,
    dependency: String? = null,
  ) {
    val directory =
      projectDir.resolve("repo/${group.replace('.', '/')}/$name/1").createDirectories()
    val urlElement = url?.let { "<url>$it</url>" }.orEmpty()
    val dependencies =
      dependency
        ?.let {
          "<dependencies><dependency><groupId>example</groupId><artifactId>$it</artifactId><version>1</version></dependency></dependencies>"
        }
        .orEmpty()
    directory
      .resolve("$name-1.pom")
      .writeText(
        """
      <project><modelVersion>4.0.0</modelVersion>
      <groupId>$group</groupId><artifactId>$name</artifactId><version>1</version>
      <licenses><license><name>$license</name>$urlElement</license></licenses>
      $dependencies</project>
      """
          .trimIndent() + "\n",
      )
    JarOutputStream(directory.resolve("$name-1.jar").outputStream()).use {}
  }

  private fun runner(privateBootstrap: Boolean = false): GradleRunner {
    val runner =
      GradleRunner.create()
        .withProjectDir(projectDir.toFile())
        .withArguments(
          "licensee",
          "--configuration-cache",
          "--offline",
          "--console=plain",
          "--no-scan",
        )
    return if (privateBootstrap) runner else runner.withPluginClasspath()
  }
}

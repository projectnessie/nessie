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

package org.projectnessie.buildtools.spotless

import com.diffplug.gradle.spotless.SpotlessExtension
import com.diffplug.gradle.spotless.SpotlessPlugin
import com.diffplug.spotless.extra.wtp.EclipseWtpFormatterStep
import org.gradle.api.Plugin
import org.gradle.api.Project
import org.gradle.kotlin.dsl.apply
import org.gradle.kotlin.dsl.configure
import org.gradle.kotlin.dsl.extra
import org.gradle.kotlin.dsl.withType

/** Applies common configurations to all Nessie projects. */
@Suppress("unused")
class SpotlessHelperPlugin : Plugin<Project> {
  override fun apply(project: Project): Unit =
    project.run {
      apply<SpotlessPlugin>()
      if (!java.lang.Boolean.getBoolean("idea.sync.active")) {
        plugins.withType<SpotlessPlugin>().configureEach {
          configure<SpotlessExtension> {
            format("xml") {
              target("src/**/*.xml", "src/**/*.xsd")
              eclipseWtp(EclipseWtpFormatterStep.XML)
                .configFile(
                  rootProject.projectDir.resolve("codestyle/org.eclipse.wst.xml.core.prefs")
                )
            }
            kotlinGradle {
              ktfmt().googleStyle()
              licenseHeaderFile(rootProject.file("codestyle/copyright-header-java.txt"), "$")
              if (project == rootProject) {
                target("*.gradle.kts", "buildSrc/*.gradle.kts")
              }
            }
            if (project == rootProject) {
              kotlin {
                ktfmt().googleStyle()
                licenseHeaderFile(rootProject.file("codestyle/copyright-header-java.txt"), "$")
                target("buildSrc/src/**/kotlin/**")
                targetExclude("buildSrc/build/**")
              }
            }

            val srcMain = projectDir.resolve("src/main")
            val srcTest = projectDir.resolve("src/test")

            if (srcMain.resolve("antlr4").exists() || srcTest.resolve("antlr4").exists()) {
              antlr4 {
                licenseHeaderFile(rootProject.file("codestyle/copyright-header-java.txt"))
                target("src/**/antlr4/**")
                targetExclude("build/**")
              }
            }
            if (srcMain.resolve("java").exists() || srcTest.resolve("java").exists()) {
              java {
                googleJavaFormat(dependencyVersion("versionGoogleJavaFormat"))
                licenseHeaderFile(rootProject.file("codestyle/copyright-header-java.txt"))
                target("src/**/java/**")
                targetExclude("build/**")
              }
            }
            if (srcMain.resolve("scala").exists() || srcTest.resolve("scala").exists()) {
              scala {
                scalafmt()
                licenseHeaderFile(
                  rootProject.file("codestyle/copyright-header-java.txt"),
                  "^(package|import) .*$"
                )
                target("src/**/scala/**")
                targetExclude("buildSrc/build/**")
              }
            }
            if (srcMain.resolve("kotlin").exists() || srcTest.resolve("kotlin").exists()) {
              kotlin {
                ktfmt().googleStyle()
                licenseHeaderFile(rootProject.file("codestyle/copyright-header-java.txt"), "$")
                target("src/**/kotlin/**")
                targetExclude("build/**")
              }
            }
          }
        }
      }
    }

  private fun Project.dependencyVersion(key: String) = rootProject.extra[key].toString()
}

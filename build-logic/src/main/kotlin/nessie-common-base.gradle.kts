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

import com.github.vlsi.jandex.JandexBuildAction
import com.github.vlsi.jandex.JandexProcessResources

plugins {
  id("com.github.vlsi.jandex")
  id("com.diffplug.spotless")
}

apply<PublishingHelperPlugin>()

jandex { toolVersion = libsRequiredVersion("jandex") }

val sourceSets: SourceSetContainer? by project

sourceSets?.withType(SourceSet::class.java)?.configureEach {
  val sourceSet = this
  if ("main" != sourceSet.name) {
    val jandexTaskName = sourceSet.getTaskName("process", "jandexIndex")
    tasks.named(jandexTaskName, JandexProcessResources::class.java).configure {
      // No Jandex for non-main
      jandexBuildAction = JandexBuildAction.NONE
      enabled = false
    }
  }
}

//

if (
  !project.extra.has("duplicated-project-sources") &&
    !System.getProperty("idea.sync.active").toBoolean()
) {
  spotless {
    format("xml") {
      target("src/**/*.xml", "src/**/*.xsd")
      eclipseWtp(com.diffplug.spotless.extra.wtp.EclipseWtpFormatterStep.XML)
        .configFile(rootProject.projectDir.resolve("codestyle/org.eclipse.wst.xml.core.prefs"))
    }
    kotlinGradle {
      ktfmt().googleStyle()
      licenseHeaderFile(rootProject.file("codestyle/copyright-header-java.txt"), "$")
      if (project == rootProject) {
        target("*.gradle.kts", "build-logic/*.gradle.kts")
      }
    }
    if (project == rootProject) {
      kotlin {
        ktfmt().googleStyle()
        licenseHeaderFile(rootProject.file("codestyle/copyright-header-java.txt"), "$")
        target("build-logic/src/**/kotlin/**")
        targetExclude("build-logic/build/**")
      }
    }

    if (project.plugins.hasPlugin("antlr")) {
      antlr4 {
        licenseHeaderFile(rootProject.file("codestyle/copyright-header-java.txt"))
        target("src/**/antlr4/**")
        targetExclude("build/**")
      }
    }
    if (project.plugins.hasPlugin("java-base")) {
      java {
        googleJavaFormat(libsRequiredVersion("googleJavaFormat"))
        licenseHeaderFile(rootProject.file("codestyle/copyright-header-java.txt"))
        target("src/**/java/**")
        targetExclude("build/**")
      }
    }
    if (project.plugins.hasPlugin("scala")) {
      scala {
        scalafmt()
        licenseHeaderFile(
          rootProject.file("codestyle/copyright-header-java.txt"),
          "^(package|import) .*$"
        )
        target("src/**/scala/**")
        targetExclude("build-logic/build/**")
      }
    }
    if (project.plugins.hasPlugin("kotlin")) {
      kotlin {
        ktfmt().googleStyle()
        licenseHeaderFile(rootProject.file("codestyle/copyright-header-java.txt"), "$")
        target("src/**/kotlin/**")
        targetExclude("build/**")
      }
    }
  }
}

//

tasks.register("compileAll").configure {
  group = "build"
  description = "Runs all compilation and jar tasks"
  dependsOn(tasks.withType<AbstractCompile>(), tasks.withType<ProcessResources>())
}

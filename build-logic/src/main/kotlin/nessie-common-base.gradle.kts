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

plugins { id("com.diffplug.spotless") }

apply<PublishingHelperPlugin>()

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
      yaml {
        // delimiter can be:
        // - a comment sign not followed by another comment sign (reserved for the license header)
        // - a YAML document separator
        // - the beginning of a YAML document (key-value pair)
        licenseHeaderFile(
          rootProject.file("codestyle/copyright-header-yaml.txt"),
          " *(#(?!#)|---|[^:#\\s\\{/]+\\s*:)",
        )
        target("helm/nessie/**/*.yaml", "helm/nessie/**/*.yml")
        targetExclude("helm/nessie/templates/**", "helm/nessie/ci/**")
      }
      format("helm-template") {
        // delimiter can be:
        // - the beginning of a template not followed by /** (reserved for the license header)
        // - a comment sign
        // - a YAML document separator
        // - the beginning of a YAML document (key-value pair)
        // - The sentence "To connect to Nessie" (NOTES.txt)
        licenseHeaderFile(
          rootProject.file("codestyle/copyright-header-helm-template.txt"),
          "( *\\{\\{(?!/\\*\\*)| *#|---|[^:#\\s\\{/]+\\s*:|To connect to Nessie)",
        )
        target("helm/nessie/templates/**")
        targetExclude("helm/nessie/templates/tests/**")
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
          "^(package|import) .*$",
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

// ensure jars conform to reproducible builds
// (https://docs.gradle.org/current/userguide/working_with_files.html#sec:reproducible_archives)
tasks.withType<AbstractArchiveTask>().configureEach {
  isPreserveFileTimestamps = false
  isReproducibleFileOrder = true
}

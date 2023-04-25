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

import org.gradle.api.JavaVersion
import org.gradle.api.Plugin
import org.gradle.api.Project
import org.gradle.api.file.DuplicatesStrategy
import org.gradle.api.plugins.JavaPlugin
import org.gradle.api.plugins.JavaPluginExtension
import org.gradle.api.tasks.bundling.Jar
import org.gradle.api.tasks.compile.JavaCompile
import org.gradle.api.tasks.javadoc.Javadoc
import org.gradle.external.javadoc.CoreJavadocOptions
import org.gradle.kotlin.dsl.configure
import org.gradle.kotlin.dsl.maven
import org.gradle.kotlin.dsl.repositories
import org.gradle.kotlin.dsl.withType

class NessieJavaPlugin : Plugin<Project> {
  override fun apply(project: Project): Unit =
    project.run {
      tasks.withType<Jar>().configureEach {
        manifest {
          attributes["Implementation-Title"] = "Nessie ${project.name}"
          attributes["Implementation-Version"] = project.version
          attributes["Implementation-Vendor"] = "Dremio"
        }
        duplicatesStrategy = DuplicatesStrategy.WARN
      }

      repositories {
        mavenCentral { content { excludeVersionByRegex("io[.]delta", ".*", ".*-nessie") } }
        maven("https://storage.googleapis.com/nessie-maven") {
          name = "Nessie Delta custom Repository"
          content { includeVersionByRegex("io[.]delta", ".*", ".*-nessie") }
        }
        if (System.getProperty("withMavenLocal").toBoolean()) {
          mavenLocal()
        }
      }

      tasks.withType<JavaCompile>().configureEach {
        options.encoding = "UTF-8"
        options.release.set(8)
        options.compilerArgs.add("-parameters")

        // Required to enable incremental compilation w/ immutables, see
        // https://github.com/immutables/immutables/pull/858 and
        // https://github.com/immutables/immutables/issues/804#issuecomment-487366544
        options.compilerArgs.add("-Aimmutables.gradle.incremental")
      }

      tasks.withType<Javadoc>().configureEach {
        val opt = options as CoreJavadocOptions
        // don't spam log w/ "warning: no @param/@return"
        opt.addStringOption("Xdoclint:-reference", "-quiet")
      }

      plugins.withType<JavaPlugin>().configureEach {
        configure<JavaPluginExtension> {
          withJavadocJar()
          withSourcesJar()
          sourceCompatibility = JavaVersion.VERSION_1_8
          targetCompatibility = JavaVersion.VERSION_1_8
        }
      }
    }
}

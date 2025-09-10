/*
 * Copyright (C) 2024 Dremio
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

import com.github.jengelman.gradle.plugins.shadow.ShadowPlugin
import org.kordamp.gradle.plugin.jandex.JandexExtension
import org.kordamp.gradle.plugin.jandex.JandexPlugin
import org.kordamp.gradle.plugin.jandex.tasks.JandexTask

plugins {
  `java-library`
  `maven-publish`
  signing
  id("org.kordamp.gradle.jandex")
  id("nessie-common-base")
  id("nessie-common-src")
  id("nessie-java")
  id("nessie-testing")
}

// Jandex

plugins.withType<JandexPlugin>().configureEach {
  extensions.getByType(JandexExtension::class).run {
    version = libsRequiredVersion("jandex")
    // https://smallrye.io/jandex/jandex/3.4.0/index.html#persistent_index_format_versions
    indexVersion = 11
  }

  tasks.withType<Javadoc>().configureEach { dependsOn("jandex") }
  tasks.named("checkstyleMain").configure { dependsOn("jandex") }
}

// Disable Jandex if a shadow jar is being built
plugins.withType<ShadowPlugin>().configureEach {
  tasks.withType<JandexTask>().configureEach { enabled = false }
}

//

if (project.hasProperty("release") || project.hasProperty("jarWithGitInfo")) {
  /**
   * Adds convenient, but not strictly necessary information to each generated "main" jar.
   *
   * This includes `pom.properties` and `pom.xml` files where Maven places those, in
   * `META-INF/maven/group-id/artifact-id/`. Also adds the `NOTICE` and `LICENSE` files in
   * `META-INF`, which makes it easier for license scanners.
   */
  plugins.withType(JavaLibraryPlugin::class.java) {
    val generatePomProperties =
      tasks.register("generatePomProperties", GeneratePomProperties::class.java) {}

    val additionalJarContent =
      tasks.register("additionalJarContent", Sync::class.java) {
        // Have to manually declare the inputs of this task here on top of the from/include below
        inputs.files(rootProject.layout.files("LICENSE", "NOTICE"))
        inputs.property(
          "groupArtifactVersion",
          "${project.group}:${project.name}:${project.version}",
        )
        dependsOn("generatePomFileForMavenPublication")
        from(rootProject.rootDir) {
          include("LICENSE", "NOTICE")
          eachFile {
            this.path =
              "META-INF/licenses/${project.group}/${project.name}-${project.version}/$sourceName"
          }
        }
        from(tasks.named("generatePomFileForMavenPublication")) {
          include("pom-default.xml")
          eachFile { this.path = "META-INF/maven/${project.group}/${project.name}/pom.xml" }
        }
        into(layout.buildDirectory.dir("license-for-jar"))
      }

    tasks.named("processResources") { dependsOn(additionalJarContent) }

    val sourceSets: SourceSetContainer by project
    sourceSets.named("main") {
      resources.srcDir(additionalJarContent)
      resources.srcDir(generatePomProperties)
    }
  }
}

configurations.all {
  rootProject
    .file("gradle/banned-dependencies.txt")
    .readText(Charsets.UTF_8)
    .trim()
    .lines()
    .map { it.trim() }
    .filterNot { it.isBlank() || it.startsWith("#") }
    .forEach { line ->
      val idx = line.indexOf(':')
      if (idx == -1) {
        exclude(group = line)
      } else {
        val group = line.substring(0, idx)
        val module = line.substring(idx + 1)
        exclude(group = group, module = module)
      }
    }
}

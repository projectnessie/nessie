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

plugins {
  `java-library`
  `maven-publish`
  signing
  id("nessie-common-base")
}

publishingHelper { mavenName = "Nessie - NOTICE + LICENSE files" }

tasks.withType<JavaCompile>().configureEach { options.release = 8 }

val noticeDir = project.layout.buildDirectory.dir("notice")

val includeNoticeLicenseFiles by
  tasks.registering(Sync::class) {
    destinationDir = noticeDir.get().asFile
    inputs.files(rootProject.layout.files("NOTICE*", "LICENSE*", "version.txt"))
    inputs.property("version", project.version)
    from(rootProject.projectDir) {
      include("NOTICE", "LICENSE-BINARY-DIST", "LICENSE")
      eachFile {
        val fileName =
          when (file.name) {
            "LICENSE" -> "LICENSE-SOURCE"
            "LICENSE-BINARY-DIST" -> "LICENSE"
            else -> file.name
          }
        path = "META-INF/resources/$fileName.txt"
      }
    }
  }

sourceSets.named("main") { resources.srcDir(noticeDir) }

tasks.named("processResources") { dependsOn(includeNoticeLicenseFiles) }

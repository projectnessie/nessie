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

extra["maven.name"] = "Nessie - NOTICE file"

tasks.withType<JavaCompile>().configureEach { options.release = 8 }

val noticeDir = project.layout.buildDirectory.dir("notice")

val includeNoticeFile by
  tasks.registering(Sync::class) {
    destinationDir = noticeDir.get().asFile
    from(rootProject.projectDir) {
      into("META-INF/resources")
      include("NOTICE")
      rename { "NOTICE.txt" }
    }
  }

sourceSets.named("main") { resources.srcDir(noticeDir) }

tasks.named("processResources") { dependsOn(includeNoticeFile) }

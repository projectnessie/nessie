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

plugins { id("nessie-conventions-java21") }

publishingHelper { mavenName = "Nessie - Quarkus Extension (Deployment)" }

dependencies {
  implementation(quarkusPlatform(project))
  annotationProcessor(quarkusPlatform(project))
  implementation("io.quarkus:quarkus-arc-deployment")
  implementation("io.quarkus:quarkus-core-deployment")
  annotationProcessor("io.quarkus:quarkus-extension-processor")
}

tasks.named<JavaCompile>("compileJava") {
  doFirst {
    // The Quarkus extension Gradle plugin emits a warning without this hack:
    // `Unable to determine artifact coordinates from: .../pom.xml`
    val pomFile = destinationDirectory.get().asFile.parentFile.parentFile.resolve("pom.xml")
    pomFile.parentFile.mkdirs()
    pomFile.writeText(
      """
      <project>
        <modelVersion>4.0.0</modelVersion>
        <groupId>${project.group}</groupId>
        <artifactId>${project.name}</artifactId>
        <name>${project.description ?: project.name}</name>
      </project>
      """
        .trimIndent() + "\n"
    )
  }
}

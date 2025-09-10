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

import com.google.protobuf.gradle.GenerateProtoTask
import com.google.protobuf.gradle.ProtobufExtension
import com.google.protobuf.gradle.ProtobufExtract

plugins {
  id("nessie-conventions-java11")
  alias(libs.plugins.protobuf)
}

publishingHelper { mavenName = "Nessie - Server - Store (Proto)" }

dependencies { api(project(path = ":nessie-protobuf-relocated", configuration = "shadow")) }

// *.proto files taken from https://github.com/googleapis/googleapis/ repo, available as a git
// submodule
extensions.configure<ProtobufExtension> {
  // Configure the protoc executable
  protoc {
    // Download from repositories
    artifact = "com.google.protobuf:protoc:${libs.versions.protobuf.get()}"
  }
}

tasks.named<GenerateProtoTask>("generateProto").configure {
  finalizedBy("updateGeneratedProtoFiles")
}

tasks.register<ReplaceInFiles>("updateGeneratedProtoFiles") {
  files.set(project.layout.buildDirectory.dir("generated/sources/proto/main"))
  replacements.put("com.google.protobuf", "org.projectnessie.nessie.relocated.protobuf")
}

// The protobuf-plugin should ideally do this
tasks.named<Jar>("sourcesJar").configure { dependsOn("generateProto") }

tasks.withType(ProtobufExtract::class).configureEach {
  when (name) {
    "extractIncludeTestProto" -> dependsOn(tasks.named("jandex"))
    "extractIncludeTestFixturesProto" -> dependsOn(tasks.named("jandex"))
    "extractIncludeIntTestProto" -> dependsOn(tasks.named("jandex"))
  }
}

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

if (!JavaVersion.current().isCompatibleWith(JavaVersion.VERSION_11)) {
  throw GradleException("Build requires Java 11")
}

pluginManagement {
  // Cannot use a settings-script global variable/value, so pass the 'versions' Properties via
  // settings.extra around.
  val versions = java.util.Properties()
  settings.extra["nessieBuildTools.versions"] = versions

  val versionIdeaExtPlugin = "1.1.4"
  versions["versionIdeaExtPlugin"] = versionIdeaExtPlugin
  val versionSpotlessPlugin = "6.7.0"
  versions["versionSpotlessPlugin"] = versionSpotlessPlugin

  plugins {
    id("com.gradle.plugin-publish") version "1.0.0-rc-2"
    id("com.diffplug.spotless") version versionSpotlessPlugin
    id("org.jetbrains.gradle.plugin.idea-ext") version versionIdeaExtPlugin
  }
  repositories {
    mavenCentral() // prefer Maven Central, in case Gradle's repo has issues
    gradlePluginPortal()
    if (java.lang.Boolean.getBoolean("withMavenLocal")) {
      mavenLocal()
    }
  }
}

gradle.rootProject {
  val prj = this
  val versions = settings.extra["nessieBuildTools.versions"] as java.util.Properties
  versions.forEach { k, v -> prj.extra[k.toString()] = v }
}

include("attach-test-jar")

include("errorprone")

include("checkstyle")

include("ide-integration")

include("publishing")

include("jandex")

include("jacoco")

include("nessie-project")

include("protobuf")

include("reflection-config")

include("smallrye-openapi")

include("spotless")

enableFeaturePreview("TYPESAFE_PROJECT_ACCESSORS")

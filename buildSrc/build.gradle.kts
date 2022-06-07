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

import java.util.Properties

plugins { `kotlin-dsl` }

repositories {
  mavenCentral()
  gradlePluginPortal()
  if (System.getProperty("withMavenLocal").toBoolean()) {
    mavenLocal()
  }
}

// Use the versions declared in the top-level settings.gradle.kts. We can safely assume that
// the properties file exists, because the top-level settings.gradle.kts is executed before
// buildSrc's settings.gradle.kts or build.gradle.kts.
val versions = Properties()

file("../build/nessieBuild/versions.properties").inputStream().use { versions.load(it) }

val versionIdeaExtPlugin = versions["versionIdeaExtPlugin"]
val versionSpotlessPlugin = versions["versionSpotlessPlugin"]
val versionNessieBuildPlugins = versions["versionNessieBuildPlugins"]
val versionErrorPronePlugin = versions["versionErrorPronePlugin"]
val versionJandexPlugin = versions["versionJandexPlugin"]
val versionQuarkus = versions["versionQuarkus"]
val versionShadowPlugin = versions["versionShadowPlugin"]

dependencies {
  implementation(gradleKotlinDsl())
  implementation("com.diffplug.spotless:spotless-plugin-gradle:$versionSpotlessPlugin")
  implementation("com.github.vlsi.gradle:jandex-plugin:$versionJandexPlugin")
  implementation("gradle.plugin.com.github.johnrengelman:shadow:$versionShadowPlugin")
  implementation("io.quarkus:gradle-application-plugin:$versionQuarkus")
  implementation("org.projectnessie.buildsupport:attach-test-jar:$versionNessieBuildPlugins")
  implementation("org.projectnessie.buildsupport:checkstyle:$versionNessieBuildPlugins")
  implementation("org.projectnessie.buildsupport:errorprone:$versionNessieBuildPlugins")
  implementation("org.projectnessie.buildsupport:ide-integration:$versionNessieBuildPlugins")
  implementation("org.projectnessie.buildsupport:jacoco:$versionNessieBuildPlugins")
  implementation("org.projectnessie.buildsupport:jandex:$versionNessieBuildPlugins")
  implementation("org.projectnessie.buildsupport:protobuf:$versionNessieBuildPlugins")
  implementation("org.projectnessie.buildsupport:publishing:$versionNessieBuildPlugins")
  implementation("org.projectnessie.buildsupport:reflection-config:$versionNessieBuildPlugins")
  implementation("org.projectnessie.buildsupport:smallrye-openapi:$versionNessieBuildPlugins")
  implementation("org.projectnessie.buildsupport:spotless:$versionNessieBuildPlugins")
}

kotlinDslPluginOptions { jvmTarget.set(JavaVersion.VERSION_11.toString()) }

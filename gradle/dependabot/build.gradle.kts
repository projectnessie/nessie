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

// Note: this is NOT a real project but a hack for dependabot to manage the plugin versions.
//
// Background: dependabot only manages dependencies (incl Gradle plugins) in build.gradle[.kts]
// files. It scans the root build.gradle[.kts] fila and those in submodules referenced in
// settings.gradle[.kts].
// But dependabot does not manage managed plugin dependencies in settings.gradle[.kts].
// However, since dependabot is a "dumb search and replace engine", we can use a trick:
// 1. Have this "dummy" build.gradle.kts file with all managed plugin dependencies.
// 2. Add an `include()` to this build file in settings.gradle.kts, surrounded with an `if (false)`,
//    so Gradle does _not_ pick it up.
// 3. Parse this file in our settings.gradle.kts, provide a `ResolutionStrategy` to the
//    plugin dependencies.

plugins {
  id("com.diffplug.spotless") version "6.11.0"
  id("com.github.johnrengelman.plugin-shadow") version "7.1.2"
  id("com.github.node-gradle.node") version "3.4.0"
  id("com.github.vlsi.jandex") version "1.82"
  id("io.gatling.gradle") version "3.8.4"
  id("io.github.gradle-nexus.publish-plugin") version "1.1.0"
  id("io.quarkus") version "2.12.3.Final"
  id("me.champeau.jmh") version "0.6.8"
  id("net.ltgt.errorprone") version "2.0.2"
  id("org.jetbrains.gradle.plugin.idea-ext") version "1.1.6"
  id("org.projectnessie") version "0.27.3"
  id("org.projectnessie.buildsupport.spotless") version "0.2.12"
  // The above version "includes" the ones below
  //  org.projectnessie.buildsupport.checkstyle
  //  org.projectnessie.buildsupport.errorprone
  //  org.projectnessie.buildsupport.ide-integration
  //  org.projectnessie.buildsupport.jacoco
  //  org.projectnessie.buildsupport.jacoco-aggregator
  //  org.projectnessie.buildsupport.jandex
  //  org.projectnessie.buildsupport.protobuf
  //  org.projectnessie.buildsupport.publishing
  //  org.projectnessie.buildsupport.reflectionconfig
  //  org.projectnessie.smallrye-open-api
}

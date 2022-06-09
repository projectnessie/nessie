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

import com.diffplug.gradle.spotless.SpotlessExtension
import com.diffplug.gradle.spotless.SpotlessPlugin

plugins {
  `java-platform`
  id("com.diffplug.spotless")
}

val versionAsm = "9.3"
val versionErrorPronePlugin = "2.0.2"
val versionIdeaExtPlugin = dependencyVersion("versionIdeaExtPlugin")
val versionJandex = "2.4.2.Final"
val versionJandexPlugin = "1.80"
val versionProtobufPlugin = "0.8.18"
val versionQuarkus = "2.9.2.Final"
val versionShadowPlugin = "7.1.2"
val versionSmallryeOpenApi = "2.1.22"
val versionSpotlessPlugin = dependencyVersion("versionSpotlessPlugin")

rootProject.extra["versionAsm"] = versionAsm

rootProject.extra["versionErrorPronePlugin"] = versionErrorPronePlugin

rootProject.extra["versionJandex"] = versionJandex

rootProject.extra["versionJandexPlugin"] = versionJandexPlugin

rootProject.extra["versionProtobufPlugin"] = versionProtobufPlugin

rootProject.extra["versionQuarkus"] = versionQuarkus

rootProject.extra["versionShadowPlugin"] = versionShadowPlugin

rootProject.extra["versionSmallryeOpenApi"] = versionSmallryeOpenApi

dependencies {
  constraints {
    api("com.diffplug.spotless:spotless-plugin-gradle:$versionSpotlessPlugin")
    api("com.github.vlsi.gradle:jandex-plugin:$versionJandexPlugin")
    api("com.google.protobuf:protobuf-gradle-plugin:$versionProtobufPlugin")
    api("gradle.plugin.com.github.johnrengelman:shadow:$versionShadowPlugin")
    api("gradle.plugin.org.jetbrains.gradle.plugin.idea-ext:gradle-idea-ext:$versionIdeaExtPlugin")
    api("io.quarkus:gradle-application-plugin:$versionQuarkus")
    api("io.smallrye:smallrye-open-api-core:$versionSmallryeOpenApi")
    api("io.smallrye:smallrye-open-api-jaxrs:$versionSmallryeOpenApi")
    api("io.smallrye:smallrye-open-api-spring:$versionSmallryeOpenApi")
    api("io.smallrye:smallrye-open-api-vertx:$versionSmallryeOpenApi")
    api("net.ltgt.gradle:gradle-errorprone-plugin:$versionErrorPronePlugin")
    api("org.ow2.asm:asm:$versionAsm")
    api("org.jboss:jandex:$versionJandex")
  }
}

javaPlatform { allowDependencies() }

allprojects {
  group = "org.projectnessie.nessie.build"

  repositories {
    gradlePluginPortal()
    mavenCentral()
    if (java.lang.Boolean.getBoolean("withMavenLocal")) {
      mavenLocal()
    }
  }

  tasks.withType<JavaCompile>().configureEach {
    targetCompatibility = JavaVersion.VERSION_11.toString()
  }

  apply<SpotlessPlugin>()
  plugins.withType<SpotlessPlugin>().configureEach {
    configure<SpotlessExtension> {
      kotlinGradle {
        ktfmt().googleStyle()
        licenseHeaderFile(rootProject.file("codestyle/copyright-header-java.txt"), "$")
      }
      if (project != rootProject) {
        kotlin {
          ktfmt().googleStyle()
          licenseHeaderFile(rootProject.file("codestyle/copyright-header-java.txt"), "$")
        }
      }
    }
  }
}

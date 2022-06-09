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

plugins {
  `kotlin-dsl`
  id("com.gradle.plugin-publish")
}

dependencies {
  implementation("io.quarkus:gradle-application-plugin:${dependencyVersion("versionQuarkus")}")
}

gradlePlugin {
  plugins {
    create("checkstyle") {
      id = "org.projectnessie.buildsupport.checkstyle"
      displayName = "Checkstyle Helper"
      implementationClass = "org.projectnessie.buildtools.checkstyle.CheckstyleHelperPlugin"
    }
  }
}

pluginBundle {
  vcsUrl = "https://github.com/projectnessie/nessie/"
  website = "https://github.com/projectnessie/nessie/"
  description = "Applies checkstyle rules, adds dependsOn-wiring for Quarkus + Jandex"
}

kotlinDslPluginOptions { jvmTarget.set(JavaVersion.VERSION_11.toString()) }

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
  implementation(
    "gradle.plugin.com.github.johnrengelman:shadow:${dependencyVersion("versionShadowPlugin")}"
  )
}

gradlePlugin {
  plugins {
    create("publishing") {
      id = "org.projectnessie.buildsupport.publishing"
      displayName = "Publishing Helper"
      implementationClass = "org.projectnessie.buildtools.publishing.PublishingHelperPlugin"
    }
  }
}

pluginBundle {
  vcsUrl = "https://github.com/projectnessie/nessie/"
  website = "https://github.com/projectnessie/nessie/"
  description = "Applies projectnessie specific maven-publishing configurations and tweaks"
}

kotlinDslPluginOptions { jvmTarget.set(JavaVersion.VERSION_11.toString()) }

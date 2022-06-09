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
    "com.google.protobuf:protobuf-gradle-plugin:${dependencyVersion("versionProtobufPlugin")}"
  )
}

gradlePlugin {
  plugins {
    create("nessie-protobuf") {
      id = "org.projectnessie.buildsupport.protobuf"
      displayName = "protobuf"
      implementationClass = "org.projectnessie.buildtools.protobuf.ProtobufHelperPlugin"
    }
  }
}

pluginBundle {
  vcsUrl = "https://github.com/projectnessie/nessie/"
  website = "https://github.com/projectnessie/nessie/"
  description =
    "Ensures that generated protobuf Java sources are visible in IntelliJ and excluded from checkstyle."
}

kotlinDslPluginOptions { jvmTarget.set(JavaVersion.VERSION_11.toString()) }

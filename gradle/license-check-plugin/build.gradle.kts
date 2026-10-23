/*
 * Copyright (C) 2026 Dremio
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
  `java-gradle-plugin`
}

group = "org.projectnessie.build"

dependencies {
  implementation(libs.licensee)
  implementation(libs.cyclonedx.plugin)
  implementation(libs.cyclonedx.core)
  testImplementation(gradleTestKit())
  testImplementation(platform(libs.junit.bom))
  testImplementation("org.junit.jupiter:junit-jupiter")
  testImplementation(libs.assertj.core)
  testImplementation("org.bouncycastle:bcpg-jdk18on:1.85")
  testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

tasks.withType<Test>().configureEach {
  useJUnitPlatform()
}

kotlin { jvmToolchain(21) }

gradlePlugin {
  plugins {
    create("nessieLicenseePolicy") {
      id = "nessie-licensee-policy"
      implementationClass = "org.projectnessie.gradle.licenses.LicenseChecksPlugin"
    }
  }
}

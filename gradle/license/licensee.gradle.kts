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

import org.projectnessie.gradle.licenses.LicenseChecksExtension
import org.projectnessie.gradle.licenses.LicenseChecksPlugin

// Keep Licensee's Maven/Plexus dependencies on the private applied-script classpath.
// An included build alone does not isolate the runtime classpath from Quarkus.
buildscript {
  repositories {
    mavenCentral()
    gradlePluginPortal()
  }
  dependencies { classpath("org.projectnessie.build:nessie-license-check-plugin") }
}

apply<LicenseChecksPlugin>()

extensions.configure<LicenseChecksExtension>("licenseChecks") {
  policyFrom(layout.settingsDirectory.file("gradle/license/license-policy.json"))
}

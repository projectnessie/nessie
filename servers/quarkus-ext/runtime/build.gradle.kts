/*
 * Copyright (C) 2024 Dremio
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
  alias(libs.plugins.quarkus.extension)
    .version(
      libs.plugins.quarkus.extension.map {
        System.getProperty("quarkus.custom.version", it.version.requiredVersion)
      }
    )
  id("nessie-conventions-java21")
}

publishingHelper { mavenName = "Nessie - Quarkus Extension (Runtime)" }

dependencies {
  implementation(quarkusPlatform(project))
  implementation("io.quarkus:quarkus-arc")
}

quarkusExtension { deploymentModule = ":nessie-quarkus-ext-deployment" }

tasks.named("validateExtension") {
  // Prevent the following error, see https://github.com/quarkusio/quarkus/issues/42054
  // Quarkus Extension Dependency Verification Error
  // The following deployment artifact(s) were found to be missing in the deployment module:
  // - io.quarkus:quarkus-arc-deployment
  // - io.quarkus:quarkus-core-deployment
  enabled = false
}

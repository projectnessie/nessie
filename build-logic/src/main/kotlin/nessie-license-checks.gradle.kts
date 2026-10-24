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

import org.gradle.api.publish.PublishingExtension
import org.gradle.api.publish.maven.MavenPublication
import org.gradle.api.publish.maven.plugins.MavenPublishPlugin

val checkDistributionLicenses =
  tasks.register("checkDistributionLicenses") {
    group = "verification"
    description = "Checks distribution dependency licenses and delivered attribution"
    dependsOn("licensee", "verifyLicenseAttribution")
  }

tasks.named("check") { dependsOn(checkDistributionLicenses) }

tasks.named("codeChecks") { dependsOn(checkDistributionLicenses) }

// Keep the former report artifact's policy gate for every Maven publication consumer,
// including bundle generation. Inventories and policy reports are not published artifacts.
plugins.withType<MavenPublishPlugin>().configureEach {
  // Discovering component artifacts can realize compilation tasks. Do this outside
  // a lazy publication configuration action, after the producer is fully configured.
  afterEvaluate {
    extensions.getByType<PublishingExtension>().publications.withType<MavenPublication>().forEach {
      publication ->
      publication.artifacts.forEach { it.builtBy(checkDistributionLicenses) }
    }
  }
}

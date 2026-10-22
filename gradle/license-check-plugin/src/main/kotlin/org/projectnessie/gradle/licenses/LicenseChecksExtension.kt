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

package org.projectnessie.gradle.licenses

import app.cash.licensee.LicenseeExtension
import javax.inject.Inject
import org.gradle.api.file.RegularFile
import org.gradle.api.provider.ProviderFactory

abstract class LicenseChecksExtension
@Inject
constructor(
  private val providers: ProviderFactory,
  private val licensee: LicenseeExtension,
) {
  /** Reads tracked policy during configuration. Subsequent Licensee DSL calls remain additive. */
  fun policyFrom(file: RegularFile) {
    val contents = providers.fileContents(file).asText.get()
    val policy = LicensePolicy(contents, file.asFile)
    policy.allowedLicenses.forEach { licensee.allow(it) }
    policy.allowedUrls.forEach { entry ->
      licensee.allowUrl(entry.getValue("url")) { because(entry.getValue("reason")) }
    }
    policy.allowedDependencies.forEach { entry ->
      licensee.allowDependency(
        entry.getValue("group"),
        entry.getValue("artifact"),
        entry.getValue("version"),
      ) {
        because(entry.getValue("reason"))
      }
    }
  }
}

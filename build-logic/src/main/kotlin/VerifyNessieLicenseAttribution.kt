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

import groovy.json.JsonSlurper
import java.util.zip.ZipFile
import org.gradle.api.DefaultTask
import org.gradle.api.GradleException
import org.gradle.api.file.ConfigurableFileCollection
import org.gradle.api.file.RegularFileProperty
import org.gradle.api.provider.Property
import org.gradle.api.provider.SetProperty
import org.gradle.api.tasks.CacheableTask
import org.gradle.api.tasks.Input
import org.gradle.api.tasks.InputFile
import org.gradle.api.tasks.InputFiles
import org.gradle.api.tasks.OutputFile
import org.gradle.api.tasks.PathSensitive
import org.gradle.api.tasks.PathSensitivity
import org.gradle.api.tasks.TaskAction

@CacheableTask
abstract class VerifyNessieLicenseAttribution : DefaultTask() {
  @get:InputFile
  @get:PathSensitive(PathSensitivity.RELATIVE)
  abstract val licenseeReport: RegularFileProperty

  @get:InputFile @get:PathSensitive(PathSensitivity.RELATIVE) abstract val sbom: RegularFileProperty

  @get:InputFile
  @get:PathSensitive(PathSensitivity.RELATIVE)
  abstract val licenseFile: RegularFileProperty

  @get:InputFile
  @get:PathSensitive(PathSensitivity.RELATIVE)
  abstract val noticeFile: RegularFileProperty

  @get:InputFiles
  @get:PathSensitive(PathSensitivity.RELATIVE)
  abstract val deliveredAttributionArchives: ConfigurableFileCollection

  @get:Input abstract val projectGroups: SetProperty<String>

  @get:Input abstract val generatedComponentNames: SetProperty<String>

  @get:Input abstract val allowEmptyExternalInventory: Property<Boolean>

  init {
    allowEmptyExternalInventory.convention(false)
  }

  @get:OutputFile abstract val validationReport: RegularFileProperty

  @TaskAction
  fun verify() {
    val checked =
      (JsonSlurper().parse(licenseeReport.get().asFile) as List<*>)
        .map { it as Map<*, *> }
        .map { coordinate(it["groupId"], it["artifactId"], it["version"]) }
        .toSortedSet()
    val components =
      (JsonSlurper().parse(sbom.get().asFile) as Map<*, *>)["components"] as? List<*>
        ?: throw GradleException("Shipped inventory does not contain components")
    val included = components.map { it as Map<*, *> }.filter { it["scope"] != "excluded" }
    val generated = included.filter { (it["purl"] as? String)?.startsWith("pkg:maven/") != true }
    require(
      generated.all {
        it["name"] in generatedComponentNames.get() &&
          (it["purl"] as? String)?.startsWith("pkg:generic/${it["name"]}@") == true
      },
    ) {
      "SBOM contains unrecognized non-Maven components: ${generated.map { it["name"] }}"
    }
    val shipped =
      (included - generated.toSet())
        .filter { it["group"] !in projectGroups.get() }
        .map { coordinate(it["group"], it["name"], it["version"]) }
        .toSortedSet()
    require(
      checked.isNotEmpty() &&
        (included - generated.toSet()).isNotEmpty() &&
        (shipped.isNotEmpty() || allowEmptyExternalInventory.get()),
    ) {
      "Expected nonempty policy evidence and shipped Maven inventory"
    }

    val license = licenseFile.get().asFile.readBytes()
    val notice = noticeFile.get().asFile.readBytes()
    val markers =
      license
        .toString(Charsets.UTF_8)
        .lineSequence()
        .filter { Regex("[A-Za-z0-9_.-]+:[A-Za-z0-9_.-]+").matches(it) }
        .toSet()
    val missingPolicy = shipped - checked
    val missingMarkers = shipped.map { it.substringBeforeLast(':') }.toSet() - markers
    val deliveryErrors = mutableListOf<String>()
    if (notice.toString(Charsets.UTF_8).isBlank()) deliveryErrors.add("NOTICE is empty")
    if (deliveredAttributionArchives.isEmpty) deliveryErrors.add("No delivered attribution archive")
    deliveredAttributionArchives.files.forEach { archive ->
      ZipFile(archive).use { zip ->
        mapOf("LICENSE" to license, "NOTICE" to notice).forEach { (name, expected) ->
          val path = "META-INF/resources/$name.txt"
          val entries = zip.entries().asSequence().filter { it.name == path }.toList()
          if (entries.size != 1) {
            deliveryErrors.add("${archive.name}: expected exactly one $path, found ${entries.size}")
          } else if (
            !zip.getInputStream(entries.single()).use { it.readBytes() }.contentEquals(expected)
          ) {
            deliveryErrors.add("${archive.name}: $path differs from attribution input")
          }
        }
      }
    }

    validationReport.get().asFile.apply {
      parentFile.mkdirs()
      writeText(
        buildString {
          appendLine("External shipped Maven coordinates: ${shipped.size}")
          appendLine("Licensee coordinates: ${checked.size}")
          appendSection("Shipped coordinates not checked by Licensee", missingPolicy)
          appendSection("Shipped coordinates missing LICENSE markers", missingMarkers)
          appendSection("Licensee coordinates absent from shipped inventory", checked - shipped)
          appendSection(
            "Generated files outside Maven inventory",
            generated.map { it["name"].toString() },
          )
          appendSection("Delivered attribution errors", deliveryErrors)
        },
      )
    }
    if (missingPolicy.isNotEmpty() || missingMarkers.isNotEmpty() || deliveryErrors.isNotEmpty()) {
      throw GradleException(
        "License attribution validation failed; see ${validationReport.get().asFile}",
      )
    }
  }

  private fun coordinate(group: Any?, artifact: Any?, version: Any?): String {
    require(
      group is String &&
        group.isNotBlank() &&
        artifact is String &&
        artifact.isNotBlank() &&
        version is String &&
        version.isNotBlank(),
    ) {
      "Expected Maven group, artifact and version"
    }
    return "$group:$artifact:$version"
  }

  private fun StringBuilder.appendSection(title: String, values: Collection<String>) {
    appendLine()
    appendLine("$title (${values.size}):")
    values.sorted().forEach { appendLine("- $it") }
  }
}

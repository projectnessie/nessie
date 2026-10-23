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

package org.projectnessie.gradle.sbom

import com.github.packageurl.PackageURLBuilder.aPackageURL
import java.io.File
import org.cyclonedx.model.Component
import org.gradle.api.DefaultTask
import org.gradle.api.file.ConfigurableFileCollection
import org.gradle.api.file.DirectoryProperty
import org.gradle.api.file.RegularFileProperty
import org.gradle.api.provider.MapProperty
import org.gradle.api.provider.Property
import org.gradle.api.tasks.*
import org.projectnessie.gradle.sbom.SbomCore.tree

/** Applies final subject identity and actual assembly inputs to a producer-generated document. */
@CacheableTask
abstract class FinalizeDistributionSbom : DefaultTask() {
  @get:InputFile @get:PathSensitive(PathSensitivity.NONE) abstract val producer: RegularFileProperty

  @get:Nested abstract val subject: Property<SbomSubject>

  @get:InputFile
  @get:Optional
  @get:PathSensitive(PathSensitivity.NONE)
  abstract val subjectFile: RegularFileProperty

  @get:InputDirectory
  @get:Optional
  @get:PathSensitive(PathSensitivity.RELATIVE)
  abstract val subjectTree: DirectoryProperty

  @get:InputFiles
  @get:PathSensitive(PathSensitivity.NONE)
  abstract val selectedFiles: ConfigurableFileCollection

  /**
   * Map of paths to maps with `group`, `name`, `version` plus optional `type` and `qualifier`
   * entries.
   */
  @get:Input abstract val metadataByPath: MapProperty<String, Map<String, String>>

  @get:Input abstract val minimized: Property<Boolean>

  @get:OutputFile abstract val document: RegularFileProperty

  @get:OutputFile @get:Optional abstract val treeDescriptor: RegularFileProperty

  init {
    minimized.convention(false)
    metadataByPath.convention(emptyMap())
  }

  // Gradle directory fingerprints omit permissions; they are part of a tree subject's identity.
  @get:Input
  val treeContents: String
    get() = if (subjectTree.isPresent) SbomCore.toJson(tree(subjectTree.get().asFile)) else ""

  @get:Input
  val descriptorName: String
    get() = treeDescriptor.orNull?.asFile?.name.orEmpty()

  @TaskAction
  fun finalizeDocument() {
    require(subjectFile.isPresent != subjectTree.isPresent) {
      "Specify exactly one file or tree subject"
    }
    require(treeDescriptor.isPresent == subjectTree.isPresent) {
      "Only a tree subject requires a descriptor"
    }
    require(!minimized.get()) { "Minimized Shadow inputs need a reviewed strategy" }
    val subject = this@FinalizeDistributionSbom.subject.get()
    val metadata = metadataByPath.get()
    val selected =
      if (
        subject.packageKind == PackageKind.SHADED_JAR ||
          !selectedFiles.isEmpty ||
          metadata.isNotEmpty()
      ) {
        require(selectedFiles.files.isNotEmpty()) { "Empty Shadow selected inputs" }
        selectedFiles.files
          .map { file ->
            selectedInputFromShadowInput(file, metadata)
          }
          .sortedBy { it.component.purl }
      } else {
        require(selectedFiles.isEmpty) { "Selected files require resolved metadata" }
        null
      }
    val tree = if (subjectTree.isPresent) treeContents else null
    val digest =
      if (tree != null) SbomCore.sha256(tree.toByteArray())
      else SbomCore.hash(subjectFile.get().asFile)

    val result =
      SbomCore.forDistribution(
        SbomCore.readBom(producer.get().asFile),
        subject,
        selected,
        digest,
        descriptorName.takeIf { it.isNotEmpty() },
      )

    SbomCore.writeBomFile(document.get().asFile, result)

    if (tree != null)
      treeDescriptor.get().asFile.apply {
        parentFile.mkdirs()
        writeText(tree)
      }
  }
}

private fun selectedInputFromShadowInput(
  file: File,
  metadata: Map<String, Map<String, String>>,
): SelectedInput {
  val input = metadata[file.absolutePath] ?: error("Unmapped Shadow input: $file")
  return SelectedInput(
    Component().apply {
      group = input.getValue("group")
      name = input.getValue("name")
      version = input.getValue("version")
      purl =
        aPackageURL()
          .withType("maven")
          .withNamespace(group)
          .withName(name)
          .withVersion(version)
          .withQualifier("type", input.getOrDefault("type", "jar"))
          .apply {
            input["classifier"]
              ?.takeIf { it.isNotEmpty() }
              ?.let {
                withQualifier("classifier", it)
              }
          }
          .build()
          .canonicalize()
    },
    SbomCore.hash(file),
  )
}

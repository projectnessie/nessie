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

import java.nio.file.Files
import java.nio.file.Path
import java.util.zip.ZipEntry
import java.util.zip.ZipOutputStream
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.gradle.api.GradleException
import org.gradle.testfixtures.ProjectBuilder
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir

class TestVerifyNessieLicenseAttribution {
  @Test
  fun permitsReviewedOwnOnlyPackagingButStillRequiresProducerAndPolicyEvidence(@TempDir dir: Path) {
    val task = fixture(dir)
    task.allowEmptyExternalInventory.set(true)
    task.sbom
      .get()
      .asFile
      .writeText(
        """{"components":[{"group":"org.projectnessie.nessie","name":"client","version":"1","purl":"pkg:maven/org.projectnessie.nessie/client@1"}]}""",
      )
    task.verify()
    assertThat(report(task)).contains("External shipped Maven coordinates: 0")
    val policy = task.licenseeReport.get().asFile.readText()
    task.licenseeReport.get().asFile.writeText("[]")
    assertThatThrownBy(task::verify).isInstanceOf(IllegalArgumentException::class.java)
    task.licenseeReport.get().asFile.writeText(policy)
    task.sbom.get().asFile.writeText("""{"components":[]}""")
    assertThatThrownBy(task::verify).isInstanceOf(IllegalArgumentException::class.java)
  }

  @Test
  fun ownOnlyPermissionDoesNotSkipExternalCoverage(@TempDir dir: Path) {
    val task = fixture(dir)
    task.allowEmptyExternalInventory.set(true)
    task.licenseFile.get().asFile.writeText("no marker\n")
    archive(task)
    assertThatThrownBy(task::verify).isInstanceOf(GradleException::class.java)
    assertThat(report(task)).contains("Shipped coordinates missing LICENSE markers (1)")
  }

  @Test
  fun validatesExternalCoverageAndBothOwnProjectGroups(@TempDir dir: Path) {
    val task = fixture(dir)
    task.verify()
    assertThat(report(task)).contains("External shipped Maven coordinates: 1")
    assertThat(report(task)).contains("Delivered attribution errors (0)")
  }

  @Test
  fun requiresPolicyEvidenceForTheShippedVersion(@TempDir dir: Path) {
    val task = fixture(dir)
    task.licenseeReport
      .get()
      .asFile
      .writeText(
        """[{"groupId":"example","artifactId":"library","version":"2"}]""",
      )
    assertThatThrownBy(task::verify).isInstanceOf(GradleException::class.java)
    assertThat(report(task))
      .contains("Shipped coordinates not checked by Licensee (1):\n- example:library:1")
  }

  @Test
  fun requiresAnExactLicenseMarker(@TempDir dir: Path) {
    val task = fixture(dir)
    task.licenseFile.get().asFile.writeText("prefix-example:library\n")
    archive(task)
    assertThatThrownBy(task::verify).isInstanceOf(GradleException::class.java)
    assertThat(report(task))
      .contains("Shipped coordinates missing LICENSE markers (1):\n- example:library")
      .contains("Delivered attribution errors (0)")
  }

  @Test
  fun rejectsBlankNoticeEvenWhenDeliveredBytesMatch(@TempDir dir: Path) {
    val task = fixture(dir)
    task.noticeFile.get().asFile.writeText(" \n")
    archive(task)
    assertThatThrownBy(task::verify).isInstanceOf(GradleException::class.java)
    assertThat(report(task)).contains("NOTICE is empty")
  }

  @Test
  fun rejectsAttributionThatWasNotDelivered(@TempDir dir: Path) {
    val task = fixture(dir)
    archive(task, notice = "different delivered notice")
    assertThatThrownBy(task::verify).isInstanceOf(GradleException::class.java)
    assertThat(report(task))
      .contains("META-INF/resources/NOTICE.txt differs from attribution input")
  }

  @Test
  fun rejectsMissingDeliveredResource(@TempDir dir: Path) {
    val task = fixture(dir)
    archive(task, includeNotice = false)
    assertThatThrownBy(task::verify).isInstanceOf(GradleException::class.java)
    assertThat(report(task)).contains("expected exactly one META-INF/resources/NOTICE.txt, found 0")
  }

  @Test
  fun rejectsUnrecognizedNonMavenComponents(@TempDir dir: Path) {
    val task = fixture(dir)
    task.sbom
      .get()
      .asFile
      .writeText(
        """{"components":[{"name":"unknown.jar","purl":"pkg:generic/unknown.jar@1"}]}""",
      )
    assertThatThrownBy(task::verify)
      .isInstanceOf(IllegalArgumentException::class.java)
      .hasMessageContaining("unrecognized non-Maven components")
  }

  @Test
  fun rejectsEmptyShippedInventory(@TempDir dir: Path) {
    val task = fixture(dir)
    task.sbom.get().asFile.writeText("""{"components":[]}""")
    assertThatThrownBy(task::verify)
      .isInstanceOf(IllegalArgumentException::class.java)
      .hasMessageContaining("nonempty policy evidence and shipped Maven inventory")
  }

  private fun fixture(dir: Path): VerifyNessieLicenseAttribution {
    val project = ProjectBuilder.builder().withProjectDir(dir.toFile()).build()
    val task =
      project.tasks.register("verifyAttribution", VerifyNessieLicenseAttribution::class.java).get()
    task.licenseeReport.set(dir.resolve("artifacts.json").toFile())
    task.sbom.set(dir.resolve("sbom.json").toFile())
    task.licenseFile.set(dir.resolve("LICENSE").toFile())
    task.noticeFile.set(dir.resolve("NOTICE").toFile())
    task.validationReport.set(dir.resolve("validation.txt").toFile())
    task.deliveredAttributionArchives.from(dir.resolve("attribution.jar").toFile())
    task.projectGroups.set(
      setOf("org.projectnessie.nessie", "org.projectnessie.nessie-integrations"),
    )
    task.generatedComponentNames.set(setOf("generated-bytecode.jar"))
    task.licenseeReport
      .get()
      .asFile
      .writeText(
        """[{"groupId":"example","artifactId":"library","version":"1"}]""",
      )
    task.sbom
      .get()
      .asFile
      .writeText(
        """
        {"components":[
          {"group":"example","name":"library","version":"1","purl":"pkg:maven/example/library@1"},
          {"group":"org.projectnessie.nessie","name":"server","version":"1","purl":"pkg:maven/org.projectnessie.nessie/server@1"},
          {"group":"org.projectnessie.nessie-integrations","name":"integration","version":"1","purl":"pkg:maven/org.projectnessie.nessie-integrations/integration@1"},
          {"name":"generated-bytecode.jar","purl":"pkg:generic/generated-bytecode.jar@1"}
        ]}
        """
          .trimIndent(),
      )
    task.licenseFile.get().asFile.writeText("example:library\n")
    task.noticeFile.get().asFile.writeText("Nessie notice\n")
    archive(task)
    return task
  }

  private fun archive(
    task: VerifyNessieLicenseAttribution,
    notice: String = task.noticeFile.get().asFile.readText(),
    includeNotice: Boolean = true,
  ) {
    ZipOutputStream(Files.newOutputStream(task.deliveredAttributionArchives.singleFile.toPath()))
      .use { zip ->
        zip.putNextEntry(ZipEntry("META-INF/resources/LICENSE.txt"))
        zip.write(task.licenseFile.get().asFile.readBytes())
        zip.closeEntry()
        if (includeNotice) {
          zip.putNextEntry(ZipEntry("META-INF/resources/NOTICE.txt"))
          zip.write(notice.toByteArray())
          zip.closeEntry()
        }
      }
  }

  private fun report(task: VerifyNessieLicenseAttribution) =
    task.validationReport.get().asFile.readText()
}

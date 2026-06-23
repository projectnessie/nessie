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

plugins { id("nessie-common-base") }

val licenseReports =
  configurations.create("licenseReports") {
    description = "Used to reference license reports"
    isCanBeConsumed = false
    isCanBeResolved = true
  }

dependencies {
  licenseReports(nessieProject("nessie-quarkus", "licenseReports"))
  licenseReports(nessieProject("nessie-server-admin-tool", "licenseReports"))
  licenseReports(nessieProject("nessie-gc-tool", "licenseReports"))
  licenseReports(nessieProject("nessie-content-generator", "licenseReports"))
  licenseReports(nessieProject("nessie-cli", "licenseReports"))

  val sparkScala = loadProperties(rootProject.file("integrations/spark-scala.properties"))
  sparkScala["sparkVersions"]
    .toString()
    .split(",")
    .map { it.trim() }
    .filter { sparkVersion -> sparkVersion.startsWith("3") }
    .forEach { sparkVersion ->
      sparkScala["sparkVersion-${sparkVersion}-scalaVersions"]
        .toString()
        .split(",")
        .map { it.trim() }
        .forEach { scalaVersion ->
          licenseReports(
            nessieProject("nessie-spark-extensions-${sparkVersion}_$scalaVersion", "licenseReports")
          )
        }
    }
}

val aggregateLicenseReports =
  tasks.register("aggregateLicenseReports", AggregateLicenseReports::class.java) {
    licenseReportZips.from(licenseReports)
    outputDir.set(layout.buildDirectory.dir("licenseReports"))
  }

val aggregatedLicenseReportsZip =
  tasks.register<Zip>("aggregatedLicenseReportsZip") {
    from(aggregateLicenseReports)
    from(rootProject.layout.projectDirectory) {
      include("NOTICE", "LICENSE")
      eachFile { path = file.name + ".txt" }
    }
    archiveExtension.set("zip")
  }

@DisableCachingByDefault(because = "Aggregating license reports is not worth caching")
abstract class AggregateLicenseReports
@Inject
constructor(
  private val fileSystemOperations: FileSystemOperations,
  private val archiveOperations: ArchiveOperations,
) : DefaultTask() {
  @get:InputFiles
  @get:PathSensitive(PathSensitivity.NAME_ONLY)
  abstract val licenseReportZips: ConfigurableFileCollection

  @get:OutputDirectory abstract val outputDir: DirectoryProperty

  @TaskAction
  fun aggregate() {
    val outputDir = outputDir.get()
    fileSystemOperations.delete { delete(outputDir) }
    licenseReportZips.files.forEach { zip ->
      val targetDirName = zip.name.replace("-license-report.zip", "")
      fileSystemOperations.copy {
        from(archiveOperations.zipTree(zip))
        into(outputDir.dir(targetDirName))
      }
    }
  }
}

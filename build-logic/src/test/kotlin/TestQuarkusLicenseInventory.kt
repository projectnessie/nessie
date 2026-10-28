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

import java.nio.file.Path
import org.assertj.core.api.Assertions.assertThat
import org.gradle.testkit.runner.GradleRunner
import org.gradle.testkit.runner.TaskOutcome
import org.junit.jupiter.api.io.TempDir
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

class TestQuarkusLicenseInventory {
  @ParameterizedTest
  @ValueSource(booleans = [false, true])
  fun restoresInventoryAfterClean(uberJar: Boolean, @TempDir dir: Path) {
    dir
      .resolve("settings.gradle")
      .toFile()
      .writeText(
        """
        rootProject.name = 'quarkus-license-fixture'
        buildCache { local { directory = file('local-cache') } }
        """
          .trimIndent() + "\n",
      )
    dir
      .resolve("build.gradle")
      .toFile()
      .writeText(
        """
        plugins {
          id 'java'
          id 'nessie-quarkus-license-checks' apply false
        }
        version = '1'
        tasks.register('codeChecks')

        // Model Quarkus's two producers: app parts generate fast-JAR inventories;
        // the final build generates uber-JAR inventories. Neither declares the side file.
        @CacheableTask
        abstract class QuarkusProducer extends DefaultTask {
          @Input abstract Property<Boolean> getGeneratesInventory()
          @Input abstract Property<String> getInventoryName()
          @OutputFile abstract RegularFileProperty getArtifact()
          @Internal abstract RegularFileProperty getInventory()

          @TaskAction void build() {
            artifact.get().asFile.with { parentFile.mkdirs(); text = 'application' }
            if (generatesInventory.get()) {
              inventory.get().asFile.text = '{"components":[]}'
            }
          }
        }
        def uber = providers.gradleProperty('uber-jar').isPresent()
        def bomName = uber ? "${'$'}{project.name}-${'$'}{project.version}-runner-cyclonedx.json" : 'quarkus-run-cyclonedx.json'
        def parts = tasks.register('quarkusAppPartsBuild', QuarkusProducer) {
          generatesInventory.set(!uber)
          inventoryName.set(bomName)
          inventory.set(layout.buildDirectory.file(bomName))
          artifact.set(layout.buildDirectory.file('parts/application.jar'))
        }
        tasks.register('quarkusBuild', QuarkusProducer) {
          dependsOn(parts)
          generatesInventory.set(uber)
          inventoryName.set(bomName)
          inventory.set(layout.buildDirectory.file(bomName))
          artifact.set(layout.buildDirectory.file('quarkus-app/application.jar'))
        }
        apply plugin: 'nessie-quarkus-license-checks'
        """
          .trimIndent() + "\n",
      )
    fun run(task: String) =
      GradleRunner.create()
        .withProjectDir(dir.toFile())
        .withPluginClasspath()
        .withArguments(
          listOf(task, "--build-cache", "--configuration-cache", "--offline", "--no-scan") +
            if (uberJar) listOf("-Puber-jar") else emptyList(),
        )
        .build()

    val producer = if (uberJar) ":quarkusBuild" else ":quarkusAppPartsBuild"
    val filename =
      if (uberJar) "quarkus-license-fixture-1-runner-cyclonedx.json"
      else "quarkus-run-cyclonedx.json"
    val inventory = dir.resolve("build/$filename").toFile()
    assertThat(run(producer).task(producer)?.outcome).isEqualTo(TaskOutcome.SUCCESS)
    val expected = inventory.readBytes()
    run("clean")
    assertThat(inventory).doesNotExist()
    val restored = run(producer)
    assertThat(restored.task(producer)?.outcome).isEqualTo(TaskOutcome.FROM_CACHE)
    assertThat(inventory).exists()
    assertThat(inventory.readBytes()).isEqualTo(expected)
    assertThat(run(producer).output)
      .contains("Configuration cache entry reused", "$producer UP-TO-DATE")
  }
}

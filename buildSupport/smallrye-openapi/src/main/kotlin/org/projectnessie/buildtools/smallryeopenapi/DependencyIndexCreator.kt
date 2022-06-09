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

package org.projectnessie.buildtools.smallryeopenapi

import java.io.File
import java.io.IOException
import java.nio.file.Files
import java.nio.file.Path
import java.time.Duration
import java.time.LocalDateTime
import java.util.*
import java.util.concurrent.Callable
import java.util.concurrent.ExecutionException
import java.util.stream.Collectors
import org.gradle.api.artifacts.PublishArtifact
import org.gradle.api.artifacts.PublishArtifactSet
import org.gradle.api.file.FileCollection
import org.gradle.api.logging.Logger
import org.jboss.jandex.*

class DependencyIndexCreator(val logger: Logger) {
  @Throws(Exception::class)
  fun createIndex(allArtifacts: PublishArtifactSet?, classesDirs: FileCollection): IndexView {
    val indexDurations: MutableList<Map.Entry<PublishArtifact, Duration>> = ArrayList()
    val artifacts: MutableList<PublishArtifact> = ArrayList()
    if (allArtifacts != null) {
      artifacts.addAll(allArtifacts)
    }
    val indexes: MutableList<IndexView> = ArrayList()

    classesDirs.files.forEach { f -> indexes.add(indexModuleClasses(f)) }

    for (artifact in artifacts) {
      if (isIgnored(artifact)) {
        continue
      }
      try {
        if (artifact.file.isDirectory) {
          // Don't' cache local worskpace artifacts. Incremental compilation in IDE's would
          // otherwise use the cached index instead of new one.
          // Right now, support for incremental compilation inside eclipse is blocked by:
          // https://github.com/eclipse-m2e/m2e-core/issues/364#issuecomment-939987848
          // target/classes
          indexes.add(indexModuleClasses(artifact))
        } else if (artifact.file.name.endsWith(".jar")) {
          val artifactIndex =
            timeAndCache(indexDurations, artifact) {
              val result = JarIndexer.createJarIndex(artifact.file, Indexer(), false, false, false)
              result.index
            }
          indexes.add(artifactIndex)
        }
      } catch (e: IOException) {
        logger.error(
          "Can't compute index of " + artifact.file.absolutePath.toString() + ", skipping",
          e
        )
      } catch (e: ExecutionException) {
        logger.error(
          "Can't compute index of " + artifact.file.absolutePath.toString() + ", skipping",
          e
        )
      }
    }
    printIndexDurations(indexDurations)
    return CompositeIndex.create(indexes)
  }

  private fun printIndexDurations(indexDurations: List<Map.Entry<PublishArtifact, Duration>>) {
    if (logger.isDebugEnabled) {
      /*
      TODO spotless fails here :(
      indexDurations.stream()
        .sorted(java.util.Map.Entry.comparingByValue())
        .filter { entry -> entry.value.toMillis() > 25 }
        .forEach { (key, value): Map.Entry<PublishArtifact, Duration> ->
          logger.debug("{} {}", buildGAVCTString(key), value)
        }
       */
    }
  }

  private fun isIgnored(artifact: PublishArtifact): Boolean {
    /*
    return if (artifact.getScope() == null && artifact.file != null) {
      // The artifact is the current mavenproject, which can never be ignored
      // file can be null for projects with packaging type pom
      false
    } else !includeDependenciesScopes.contains(artifact.getScope())
      || !includeDependenciesTypes.contains(artifact.type)
      || ignoredArtifacts.contains(artifact.getGroupId())
      || ignoredArtifacts.contains(artifact.getGroupId().toString() + ":" + artifact.getArtifactId())
     */
    return false // TODO
  }

  @Throws(Exception::class)
  private fun timeAndCache(
    indexDurations: MutableList<Map.Entry<PublishArtifact, Duration>>,
    artifact: PublishArtifact,
    callable: Callable<IndexView>
  ): IndexView {
    val start: LocalDateTime = LocalDateTime.now()
    val result: IndexView =
      callable.call() // TODO indexCache.get(buildGAVCTString(artifact), callable)
    val end: LocalDateTime = LocalDateTime.now()
    val duration: Duration = Duration.between(start, end)
    indexDurations.add(AbstractMap.SimpleEntry(artifact, duration))
    return result
  }

  @Throws(IOException::class)
  private fun indexModuleClasses(artifact: PublishArtifact): Index {
    return indexModuleClasses(artifact.file)
  }

  @Throws(IOException::class)
  private fun indexModuleClasses(file: File): Index {
    val indexer = Indexer()

    // Check first if the classes directory exists, before attempting to create an index for the
    // classes
    if (file.exists()) {
      Files.walk(file.toPath()).use { stream ->
        val classFiles: List<Path> =
          stream.filter { path -> path.toString().endsWith(".class") }.collect(Collectors.toList())
        for (path in classFiles) {
          indexer.index(Files.newInputStream(path))
        }
      }
    }
    return indexer.complete()
  }

  private fun buildGAVCTString(artifact: PublishArtifact): String {
    return artifact.name +
      ":" +
      // artifact.getGroupId().toString() +
      // ":" +
      // artifact.getArtifactId() +
      // ":" +
      // artifact.getVersion() +
      // ":" +
      artifact.classifier +
      ":" +
      artifact.type
  }
}

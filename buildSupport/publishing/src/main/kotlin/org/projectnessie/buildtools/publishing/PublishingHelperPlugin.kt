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

package org.projectnessie.buildtools.publishing

import com.github.jengelman.gradle.plugins.shadow.ShadowExtension
import groovy.util.Node
import groovy.util.NodeList
import org.gradle.api.GradleException
import org.gradle.api.Plugin
import org.gradle.api.Project
import org.gradle.api.artifacts.Configuration
import org.gradle.api.artifacts.ProjectDependency
import org.gradle.api.artifacts.component.ModuleComponentSelector
import org.gradle.api.artifacts.result.DependencyResult
import org.gradle.api.component.SoftwareComponentContainer
import org.gradle.api.internal.artifacts.dependencies.DefaultProjectDependencyConstraint
import org.gradle.api.internal.component.SoftwareComponentInternal
import org.gradle.api.publish.PublishingExtension
import org.gradle.api.publish.maven.MavenPublication
import org.gradle.api.publish.maven.plugins.MavenPublishPlugin
import org.gradle.api.publish.tasks.GenerateModuleMetadata
import org.gradle.api.tasks.PathSensitivity
import org.gradle.kotlin.dsl.configure
import org.gradle.kotlin.dsl.extra
import org.gradle.kotlin.dsl.provideDelegate
import org.gradle.kotlin.dsl.register
import org.gradle.kotlin.dsl.withType
import org.gradle.plugins.signing.SigningExtension
import org.gradle.plugins.signing.SigningPlugin

/** Applies common configurations to all Nessie projects. */
@Suppress("unused")
class PublishingHelperPlugin : Plugin<Project> {
  override fun apply(project: Project): Unit =
    project.run {
      extensions.create("publishingHelper", PublishingHelperExtension::class.java, this)

      plugins.withType<MavenPublishPlugin>().configureEach {
        configure<PublishingExtension> {
          publications {
            register<MavenPublication>("maven") {
              val shadowExtension = project.extensions.findByType(ShadowExtension::class.java)
              if (shadowExtension != null) {
                shadowExtension.component(this)
              } else {
                from(components.firstOrNull { c -> c.name == "javaPlatform" || c.name == "java" })
              }
              suppressPomMetadataWarningsFor("testApiElements")
              suppressPomMetadataWarningsFor("testJavadocElements")
              suppressPomMetadataWarningsFor("testRuntimeElements")
              suppressPomMetadataWarningsFor("testSourcesElements")

              // Must do all the following behind a project.afterEvaluate{}, because
              // `mavenArtifactId()` requires the build.gradle.kts file to configure the
              // `extra["maven.artifactId"]`-property.
              afterEvaluate {
                groupId = "$group"
                artifactId = mavenArtifactId()
                version = project.version.toString()

                tasks.named("generatePomFileForMavenPublication") {
                  val e = project.extensions.getByType(PublishingHelperExtension::class.java)

                  pom {
                    name.set(
                      project.provider {
                        if (project.extra.has("maven.name")) {
                          project.extra["maven.name"].toString()
                        } else {
                          project.name
                        }
                      }
                    )
                    description.set(project.description)
                    if (project != rootProject) {
                      withXml {
                        val projectNode = asNode()

                        val parentNode = projectNode.appendNode("parent")
                        parentNode.appendNode("groupId", parent!!.group)
                        parentNode.appendNode("artifactId", parent!!.mavenArtifactId())
                        parentNode.appendNode("version", parent!!.version)

                        addMissingMandatoryDependencyVersions(projectNode)

                        fixTestJarDependencyType(projectNode, components)
                      }
                    } else {
                      val nessieRepoName = e.nessieRepoName.get()

                      inputs
                        .file(rootProject.file("gradle/developers.csv"))
                        .withPathSensitivity(PathSensitivity.RELATIVE)
                      inputs
                        .file(rootProject.file("gradle/contributors.csv"))
                        .withPathSensitivity(PathSensitivity.RELATIVE)
                      doFirst {
                        inceptionYear.set(e.inceptionYear.get())
                        url.set("https://github.com/projectnessie/$nessieRepoName")
                        organization {
                          name.set("Project Nessie")
                          url.set("https://projectnessie.org")
                        }
                        licenses {
                          license {
                            name.set("The Apache License, Version 2.0")
                            url.set("https://www.apache.org/licenses/LICENSE-2.0.txt")
                          }
                        }
                        mailingLists {
                          mailingList {
                            name.set("Project Nessie List")
                            subscribe.set("projectnessie-subscribe@googlegroups.com")
                            unsubscribe.set("projectnessie-unsubscribe@googlegroups.com")
                            post.set("projectnessie@googlegroups.com")
                            archive.set("https://groups.google.com/g/projectnessie")
                          }
                        }
                        scm {
                          connection.set("scm:git:https://github.com/projectnessie/$nessieRepoName")
                          developerConnection.set(
                            "scm:git:https://github.com/projectnessie/$nessieRepoName"
                          )
                          url.set("https://github.com/projectnessie/$nessieRepoName/tree/main")
                          tag.set("main")
                        }
                        issueManagement {
                          system.set("Github")
                          url.set("https://github.com/projectnessie/$nessieRepoName/issues")
                        }
                        developers {
                          file(rootProject.file("gradle/developers.csv"))
                            .readLines()
                            .map { line -> line.trim() }
                            .filter { line -> line.isNotEmpty() && !line.startsWith("#") }
                            .forEach { line ->
                              val args = line.split(",")
                              if (args.size < 3) {
                                throw GradleException(
                                  "gradle/developers.csv contains invalid line '${line}'"
                                )
                              }
                              developer {
                                id.set(args[0])
                                name.set(args[1])
                                url.set(args[2])
                              }
                            }
                        }
                        contributors {
                          file(rootProject.file("gradle/contributors.csv"))
                            .readLines()
                            .map { line -> line.trim() }
                            .filter { line -> line.isNotEmpty() && !line.startsWith("#") }
                            .forEach { line ->
                              val args = line.split(",")
                              if (args.size > 2) {
                                throw GradleException(
                                  "gradle/contributors.csv contains invalid line '${line}'"
                                )
                              }
                              contributor {
                                name.set(args[0])
                                url.set(args[1])
                              }
                            }
                        }
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }

      // Gradle complains when a Gradle module metadata ("pom on steroids") is generated with an
      // enforcedPlatform() dependency - but Quarkus requires enforcedPlatform(), so we have to
      // allow it.
      tasks.withType<GenerateModuleMetadata>().configureEach {
        suppressedValidationErrors.add("enforced-platform")
      }

      if (project.hasProperty("release")) {
        plugins.withType<SigningPlugin>().configureEach {
          configure<SigningExtension> {
            val signingKey: String? by project
            val signingPassword: String? by project
            useInMemoryPgpKeys(signingKey, signingPassword)
            val publishing = project.extensions.getByType(PublishingExtension::class.java)
            afterEvaluate { sign(publishing.publications.getByName("maven")) }
          }
        }
      }
    }

  /**
   * Scans the generated pom.xml for `<dependencies>` in `<dependencyManagement>` that do not have a
   * `<version>` and adds one, if possible. Maven kinda requires `<version>` tags there, even if the
   * `<dependency>` without a `<version>` is a bom and that bom's version is available transitively.
   */
  private fun Project.addMissingMandatoryDependencyVersions(projectNode: Node) {
    xmlNode(xmlNode(projectNode, "dependencyManagement"), "dependencies")?.children()?.forEach {
      val dependency = it as Node
      if (xmlNode(dependency, "version") == null) {
        val depGroup = xmlNode(dependency, "groupId")!!.text()
        val depName = xmlNode(dependency, "artifactId")!!.text()

        var depResult =
          findDependency(configurations.findByName("runtimeClasspath"), depGroup, depName)
        if (depResult == null) {
          depResult =
            findDependency(configurations.findByName("testRuntimeClasspath"), depGroup, depName)
        }

        if (depResult != null) {
          val req = depResult.requested as ModuleComponentSelector
          dependency.appendNode("version", req.version)
        }
      }
    }
  }

  private fun findDependency(
    config: Configuration?,
    depGroup: String,
    depName: String
  ): DependencyResult? {
    if (config != null) {
      val depResult =
        config.incoming.resolutionResult.allDependencies.find { depResult ->
          val req = depResult.requested
          if (req is ModuleComponentSelector) req.group == depGroup && req.module == depName
          else false
        }
      return depResult
    }
    return null
  }

  /**
   * We need the "test-jar"-type artifacts in the Maven poms, but Gradle supports no way to "mimic"
   * that Maven behavior.
   *
   * The `AttachTestJarPlugin` registers a Gradle feature "tests", which is properly respected by
   * the Gradle module metadata via the project dependencies with the required capabilities to the
   * "tests" feature.
   *
   * This function maps project dependencies to a "-tests" requested capability to the Maven
   * `<type>test-jar</type>` dependency notion.
   */
  private fun fixTestJarDependencyType(projectNode: Node, components: SoftwareComponentContainer) {
    val softwareComponent =
      components.firstOrNull { c -> c.name == "javaPlatform" || c.name == "java" }
    if (softwareComponent is SoftwareComponentInternal) {
      fixTestJarDependencyType(softwareComponent, xmlNode(projectNode, "dependencies"))
      fixTestJarDependencyType(
        softwareComponent,
        xmlNode(xmlNode(projectNode, "dependencyManagement"), "dependencies")
      )
    }
  }

  private fun fixTestJarDependencyType(
    softwareComponent: SoftwareComponentInternal,
    dependencies: Node?
  ) {
    if (dependencies != null) {
      softwareComponent.usages.forEach { usage ->
        val fromDependencyConstraints =
          usage.dependencyConstraints.filterIsInstance<DefaultProjectDependencyConstraint>().map {
            it.projectDependency
          }
        val fromDependencies = usage.dependencies.filterIsInstance<ProjectDependency>()
        (fromDependencyConstraints + fromDependencies).forEach { depPrj ->
          val capability = depPrj.requestedCapabilities.find { c -> c.name.endsWith("-tests") }
          if (capability != null) {
            val dependency =
              dependencies.children().firstOrNull { dependency ->
                dependency as Node
                val depGroup = xmlNode(dependency, "groupId")!!.text()
                val depArtifact = xmlNode(dependency, "artifactId")!!.text()
                depGroup == depPrj.group &&
                  depArtifact == depPrj.dependencyProject.mavenArtifactId()
              } as Node?
            if (dependency != null) {
              if ((dependency["type"] as NodeList).isEmpty()) {
                dependency.appendNode("type", "test-jar")
              }
            }
          }
        }
      }
    }
  }

  private fun xmlNode(node: Node?, child: String): Node? {
    val found = node?.get(child)
    if (found is NodeList) {
      if (found.isNotEmpty()) {
        return found[0] as Node
      }
    }
    return null
  }

  private fun Project.mavenArtifactId(): String {
    if (extra.has("maven.artifactId")) {
      return extra["maven.artifactId"] as String
    }

    var n = name
    var prj = this
    if (prj != rootProject) {
      while (true) {
        prj = prj.parent!!
        n = "${prj.name}-$n"
        if (prj.parent == null) {
          break
        }
      }
    }
    return n
  }

  private fun Project.dependencyVersion(key: String) = rootProject.extra[key].toString()
}

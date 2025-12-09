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

import com.github.jengelman.gradle.plugins.shadow.ShadowPlugin
import groovy.util.Node
import groovy.util.NodeList
import javax.inject.Inject
import org.gradle.api.GradleException
import org.gradle.api.Plugin
import org.gradle.api.Project
import org.gradle.api.artifacts.Configuration
import org.gradle.api.artifacts.ConfigurationVariant
import org.gradle.api.artifacts.ProjectDependency
import org.gradle.api.artifacts.component.ModuleComponentSelector
import org.gradle.api.artifacts.result.DependencyResult
import org.gradle.api.attributes.Bundling
import org.gradle.api.attributes.Category
import org.gradle.api.attributes.LibraryElements
import org.gradle.api.attributes.Usage
import org.gradle.api.component.AdhocComponentWithVariants
import org.gradle.api.component.SoftwareComponentFactory
import org.gradle.api.plugins.JavaBasePlugin
import org.gradle.api.provider.Property
import org.gradle.api.publish.PublishingExtension
import org.gradle.api.publish.maven.MavenPublication
import org.gradle.api.publish.maven.plugins.MavenPublishPlugin
import org.gradle.api.publish.tasks.GenerateModuleMetadata
import org.gradle.api.tasks.PathSensitivity
import org.gradle.kotlin.dsl.configure
import org.gradle.kotlin.dsl.provideDelegate
import org.gradle.kotlin.dsl.register
import org.gradle.kotlin.dsl.withType
import org.gradle.plugins.signing.SigningExtension
import org.gradle.plugins.signing.SigningPlugin

/** Applies common configurations to all Nessie projects. */
@Suppress("unused")
class PublishingHelperPlugin
@Inject
constructor(private val softwareComponentFactory: SoftwareComponentFactory) : Plugin<Project> {
  override fun apply(project: Project): Unit =
    project.run {
      extensions.create("publishingHelper", PublishingHelperExtension::class.java)

      plugins.withType<MavenPublishPlugin>().configureEach {
        configure<PublishingExtension> {
          publications {
            register<MavenPublication>("maven") {
              val mavenPublication = this
              afterEvaluate {
                // This MUST happen in an 'afterEvaluate' to ensure that the Shadow*Plugin has
                // been applied.
                if (project.plugins.hasPlugin(ShadowPlugin::class.java)) {
                  configureShadowPublishing(project, mavenPublication)
                } else {
                  val component =
                    components.firstOrNull { c -> c.name == "javaPlatform" || c.name == "java" }
                  if (component is AdhocComponentWithVariants) {
                    listOf("testFixturesApiElements", "testFixturesRuntimeElements").forEach { cfg
                      ->
                      configurations.findByName(cfg)?.apply {
                        component.addVariantsFromConfiguration(this) { skip() }
                      }
                    }
                  }
                  from(component)
                }

                suppressPomMetadataWarningsFor("testApiElements")
                suppressPomMetadataWarningsFor("testJavadocElements")
                suppressPomMetadataWarningsFor("testRuntimeElements")
                suppressPomMetadataWarningsFor("testSourcesElements")

                mavenPublication.groupId = "$group"
                mavenPublication.version = project.version.toString()
              }

              tasks.named("generatePomFileForMavenPublication").configure {
                pom {
                  val ep = project.extensions.getByType(PublishingHelperExtension::class.java)
                  name.set(ep.mavenName.orElse(project.name))
                  description.set(project.description)
                  if (project != rootProject) {
                    withXml {
                      val projectNode = asNode()

                      val parentNode = projectNode.appendNode("parent")
                      parentNode.appendNode("groupId", parent!!.group)
                      parentNode.appendNode("artifactId", parent!!.name)
                      parentNode.appendNode("version", parent!!.version)

                      addMissingMandatoryDependencyVersions(projectNode)
                    }
                  }

                  inputs
                    .file(rootProject.file("gradle/developers.csv"))
                    .withPathSensitivity(PathSensitivity.RELATIVE)
                  inputs
                    .file(rootProject.file("gradle/contributors.csv"))
                    .withPathSensitivity(PathSensitivity.RELATIVE)
                  doFirst {
                    val e = rootProject.extensions.getByType(PublishingHelperExtension::class.java)

                    val nessieRepoName = e.nessieRepoName.get()

                    inceptionYear.set(e.inceptionYear.get())
                    url.set("https://github.com/projectnessie/$nessieRepoName")
                    organization {
                      name.set("Project Nessie")
                      url.set("https://projectnessie.org")
                    }
                    licenses {
                      license {
                        name.set("Apache-2.0") // SPDX-ID
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
                      val version = project.version.toString()
                      if (!version.endsWith("-SNAPSHOT")) {
                        tag.set("nessie-$version")
                      }
                    }
                    issueManagement {
                      system.set("GitHub")
                      url.set("https://github.com/projectnessie/$nessieRepoName/issues")
                    }
                    developers {
                      rootProject.layout.projectDirectory
                        .file("gradle/developers.csv")
                        .asFile
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
                      rootProject.layout.projectDirectory
                        .file("gradle/contributors.csv")
                        .asFile
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

            if (project.hasProperty("useGpgAgent")) {
              useGpgCmd()
            }
          }
        }
      }
    }

  /**
   * "Proper" publication of shadow-jar instead of the "main" jar, with "the right" Gradle's module
   * metadata that refers to the shadow-jar instead of the "main" jar, which is not published by
   * Nessie.
   *
   * Pieces of this function are taken from the `Java(Base)Plugin` and `ShadowExtension`.
   */
  private fun configureShadowPublishing(project: Project, mavenPublication: MavenPublication) {
    fun isPublishable(element: ConfigurationVariant): Boolean {
      for (artifact in element.artifacts) {
        if (JavaBasePlugin.UNPUBLISHABLE_VARIANT_ARTIFACTS.contains(artifact.type)) {
          return false
        }
      }
      return true
    }

    val shadowJar = project.tasks.named("shadowJar")

    val shadowApiElements =
      project.configurations.create("shadowApiElements") {
        isCanBeConsumed = true
        isCanBeResolved = false
        attributes {
          attribute(Usage.USAGE_ATTRIBUTE, project.objects.named(Usage::class.java, Usage.JAVA_API))
          attribute(
            Category.CATEGORY_ATTRIBUTE,
            project.objects.named(Category::class.java, Category.LIBRARY),
          )
          attribute(
            LibraryElements.LIBRARY_ELEMENTS_ATTRIBUTE,
            project.objects.named(LibraryElements::class.java, LibraryElements.JAR),
          )
          attribute(
            Bundling.BUNDLING_ATTRIBUTE,
            project.objects.named(Bundling::class.java, Bundling.SHADOWED),
          )
        }
        outgoing.artifact(shadowJar)
      }

    val component = softwareComponentFactory.adhoc("shadow")
    component.addVariantsFromConfiguration(shadowApiElements) {
      if (isPublishable(configurationVariant)) {
        mapToMavenScope("compile")
      } else {
        skip()
      }
    }
    // component.addVariantsFromConfiguration(configurations.getByName("runtimeElements")) {
    component.addVariantsFromConfiguration(
      project.configurations.getByName("shadowRuntimeElements")
    ) {
      if (isPublishable(configurationVariant)) {
        mapToMavenScope("runtime")
      } else {
        skip()
      }
    }
    // Sonatype requires the javadoc and sources jar to be present, but the
    // Shadow extension does not publish those.
    component.addVariantsFromConfiguration(project.configurations.getByName("javadocElements")) {}
    component.addVariantsFromConfiguration(project.configurations.getByName("sourcesElements")) {}
    mavenPublication.from(component)

    // This a replacement to add dependencies to the pom, if necessary. Equivalent to
    // 'shadowExtension.component(mavenPublication)', which we cannot use.

    mavenPublication.pom {
      withXml {
        val node = asNode()
        val depNode = node.get("dependencies")
        val dependenciesNode =
          if ((depNode as NodeList).isNotEmpty()) depNode[0] as Node
          else node.appendNode("dependencies")
        project.configurations.getByName("shadow").allDependencies.forEach {
          if (it is ProjectDependency) {
            val dependencyNode = dependenciesNode.appendNode("dependency")
            dependencyNode.appendNode("groupId", it.group)
            dependencyNode.appendNode("artifactId", it.name)
            dependencyNode.appendNode("version", it.version)
            dependencyNode.appendNode("scope", "runtime")
          }
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
    depName: String,
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

  private fun xmlNode(node: Node?, child: String): Node? {
    val found = node?.get(child)
    if (found is NodeList) {
      if (found.isNotEmpty()) {
        return found[0] as Node
      }
    }
    return null
  }
}

abstract class PublishingHelperExtension {
  abstract val mavenName: Property<String>
  abstract val nessieRepoName: Property<String>
  abstract val inceptionYear: Property<String>
}

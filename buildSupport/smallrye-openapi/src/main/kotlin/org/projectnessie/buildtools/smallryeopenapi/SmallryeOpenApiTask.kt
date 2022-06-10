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

import io.smallrye.openapi.api.OpenApiConfig
import io.smallrye.openapi.api.OpenApiDocument
import io.smallrye.openapi.api.constants.OpenApiConstants
import io.smallrye.openapi.runtime.OpenApiProcessor
import io.smallrye.openapi.runtime.OpenApiStaticFile
import io.smallrye.openapi.runtime.io.Format
import io.smallrye.openapi.runtime.io.OpenApiSerializer
import io.smallrye.openapi.runtime.scanner.OpenApiAnnotationScanner
import java.io.File
import java.io.IOException
import java.net.MalformedURLException
import java.net.URL
import java.net.URLClassLoader
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.nio.file.StandardOpenOption
import java.util.*
import java.util.stream.Collectors
import javax.inject.Inject
import org.eclipse.microprofile.openapi.OASConfig
import org.eclipse.microprofile.openapi.models.OpenAPI
import org.gradle.api.DefaultTask
import org.gradle.api.GradleException
import org.gradle.api.NamedDomainObjectProvider
import org.gradle.api.artifacts.Configuration
import org.gradle.api.file.FileCollection
import org.gradle.api.tasks.*
import org.gradle.api.tasks.Optional
import org.gradle.kotlin.dsl.listProperty
import org.gradle.kotlin.dsl.property
import org.jboss.jandex.IndexView

@CacheableTask
abstract class SmallryeOpenApiTask
@Inject
constructor(
  ext: SmallryeOpenApiExtension,
  private val configProvider: NamedDomainObjectProvider<Configuration>,
  private val resourcesSrcDirs: FileCollection,
  private val classesDirs: FileCollection
) : DefaultTask() {

  @TaskAction
  fun generate() {
    try {
      clearOutput()

      val config = configProvider.get()

      val allArtifacts = if (scanDependenciesDisable.get()) config.allArtifacts else null

      val index: IndexView = DependencyIndexCreator(logger).createIndex(allArtifacts, classesDirs)
      val schema: OpenApiDocument = generateSchema(index, resourcesSrcDirs, config)
      write(schema)
    } catch (ex: Exception) {
      throw GradleException(
        "Could not generate OpenAPI Schema",
        ex
      ) // TODO allow failOnError = false ?
    }
  }

  @Throws(IOException::class)
  private fun generateSchema(
    index: IndexView,
    resourcesSrcDirs: FileCollection,
    config: FileCollection
  ): OpenApiDocument {
    val properties = getProperties()
    val openApiConfig: OpenApiConfig = GradleConfig(properties)
    val staticModel: OpenAPI? = generateStaticModel(resourcesSrcDirs)
    val annotationModel: OpenAPI? = generateAnnotationModel(index, openApiConfig)
    val classLoader = getClassLoader(config)
    val readerModel: OpenAPI? = OpenApiProcessor.modelFromReader(openApiConfig, classLoader)

    logger.info(
      "Generating OpenAPI schema using properties:{}",
      properties.entries
        .stream()
        .sorted(java.util.Map.Entry.comparingByKey())
        .map { e -> "${e.key} = ${e.value}" }
        .collect(Collectors.joining("\n    ", "\n    ", ""))
    )

    val document: OpenApiDocument = OpenApiDocument.INSTANCE
    document.reset()
    document.config(openApiConfig)
    if (annotationModel != null) {
      document.modelFromAnnotations(annotationModel)
    }
    if (readerModel != null) {
      document.modelFromReader(readerModel)
    }
    if (staticModel != null) {
      document.modelFromStaticFile(staticModel)
    }
    document.filter(OpenApiProcessor.getFilter(openApiConfig, classLoader))
    document.initialize()
    return document
  }

  @Throws(MalformedURLException::class)
  private fun getClassLoader(config: FileCollection): ClassLoader {
    val urls: MutableSet<URL> = HashSet()
    for (dependency in config.files) {
      urls.add(dependency.toURI().toURL())
    }

    return URLClassLoader.newInstance(
      urls.toTypedArray(),
      Thread.currentThread().contextClassLoader
    )
  }

  private fun generateAnnotationModel(
    indexView: IndexView,
    openApiConfig: OpenApiConfig
  ): OpenAPI? {
    val openApiAnnotationScanner = OpenApiAnnotationScanner(openApiConfig, indexView)
    return openApiAnnotationScanner.scan()
  }

  @Throws(IOException::class)
  private fun generateStaticModel(resourcesSrcDirs: FileCollection): OpenAPI? {
    val staticFile =
      resourcesSrcDirs.files
        .stream()
        .map { dir -> getStaticFile(dir) }
        .filter { staticFile -> staticFile != null }
        .findFirst()
        .orElse(null)
    if (staticFile != null) {
      Files.newInputStream(staticFile).use { `is` ->
        OpenApiStaticFile(`is`, getFormat(staticFile)).use { openApiStaticFile ->
          return OpenApiProcessor.modelFromStaticFile(openApiStaticFile)
        }
      }
    }
    return null
  }

  private fun getStaticFile(dir: File): Path? {
    logger.debug("Checking for static file in {}", dir)
    val classesPath: Path = dir.toPath()
    if (Files.exists(classesPath)) {
      var resourcePath: Path = Paths.get(classesPath.toString(), META_INF_OPENAPI_YAML)
      if (Files.exists(resourcePath)) {
        return resourcePath
      }
      resourcePath = Paths.get(classesPath.toString(), WEB_INF_CLASSES_META_INF_OPENAPI_YAML)
      if (Files.exists(resourcePath)) {
        return resourcePath
      }
      resourcePath = Paths.get(classesPath.toString(), META_INF_OPENAPI_YML)
      if (Files.exists(resourcePath)) {
        return resourcePath
      }
      resourcePath = Paths.get(classesPath.toString(), WEB_INF_CLASSES_META_INF_OPENAPI_YML)
      if (Files.exists(resourcePath)) {
        return resourcePath
      }
      resourcePath = Paths.get(classesPath.toString(), META_INF_OPENAPI_JSON)
      if (Files.exists(resourcePath)) {
        return resourcePath
      }
      resourcePath = Paths.get(classesPath.toString(), WEB_INF_CLASSES_META_INF_OPENAPI_JSON)
      if (Files.exists(resourcePath)) {
        return resourcePath
      }
    }
    return null
  }

  private fun getFormat(path: Path): Format {
    return if (path.endsWith(".json")) {
      Format.JSON
    } else Format.YAML
  }

  @Throws(IOException::class)
  private fun getProperties(): Map<String, String> {
    // First check if the configProperties is set, if so, load that.
    val cp: MutableMap<String, String> = HashMap()
    if (configProperties.isPresent) {
      val configPropsFile = configProperties.get().asFile
      if (configPropsFile.exists()) {
        val p = Properties()
        Files.newInputStream(configPropsFile.toPath()).use { `is` ->
          p.load(`is`)
          p.forEach { k, v -> cp[k.toString()] = v.toString() }
        }
      }
    }

    // Now add properties set in the maven plugin.
    addToPropertyMap(cp, OASConfig.MODEL_READER, modelReader.orNull)
    addToPropertyMap(cp, OASConfig.FILTER, filter.orNull)
    addToPropertyMap(cp, OASConfig.SCAN_DISABLE, scanDisabled.orNull)
    addToPropertyMap(cp, OASConfig.SCAN_PACKAGES, scanPackages.orNull)
    addToPropertyMap(cp, OASConfig.SCAN_CLASSES, scanClasses.orNull)
    addToPropertyMap(cp, OASConfig.SCAN_EXCLUDE_PACKAGES, scanExcludePackages.orNull)
    addToPropertyMap(cp, OASConfig.SCAN_EXCLUDE_CLASSES, scanExcludeClasses.orNull)
    addToPropertyMap(cp, OASConfig.SERVERS, servers.orNull)
    addToPropertyMap(cp, OASConfig.SERVERS_PATH_PREFIX, pathServers.orNull)
    addToPropertyMap(cp, OASConfig.SERVERS_OPERATION_PREFIX, operationServers.orNull)
    addToPropertyMap(
      cp,
      OpenApiConstants.SMALLRYE_SCAN_DEPENDENCIES_DISABLE,
      scanDependenciesDisable.orNull
    )
    addToPropertyMap(
      cp,
      OpenApiConstants.SMALLRYE_CUSTOM_SCHEMA_REGISTRY_CLASS,
      customSchemaRegistryClass.orNull
    )
    addToPropertyMap(cp, OpenApiConstants.SMALLRYE_APP_PATH_DISABLE, applicationPathDisable.orNull)
    addToPropertyMap(cp, OpenApiConstants.VERSION, openApiVersion.orNull)
    addToPropertyMap(cp, OpenApiConstants.INFO_TITLE, infoTitle.orNull)
    addToPropertyMap(cp, OpenApiConstants.INFO_VERSION, infoVersion.orNull)
    addToPropertyMap(cp, OpenApiConstants.INFO_DESCRIPTION, infoDescription.orNull)
    addToPropertyMap(cp, OpenApiConstants.INFO_TERMS, infoTermsOfService.orNull)
    addToPropertyMap(cp, OpenApiConstants.INFO_CONTACT_EMAIL, infoContactEmail.orNull)
    addToPropertyMap(cp, OpenApiConstants.INFO_CONTACT_NAME, infoContactName.orNull)
    addToPropertyMap(cp, OpenApiConstants.INFO_CONTACT_URL, infoContactUrl.orNull)
    addToPropertyMap(cp, OpenApiConstants.INFO_LICENSE_NAME, infoLicenseName.orNull)
    addToPropertyMap(cp, OpenApiConstants.INFO_LICENSE_URL, infoLicenseUrl.orNull)
    addToPropertyMap(cp, OpenApiConstants.OPERATION_ID_STRAGEGY, operationIdStrategy.orNull)
    addToPropertyMap(cp, OpenApiConstants.SCAN_PROFILES, scanProfiles.orNull)
    addToPropertyMap(cp, OpenApiConstants.SCAN_EXCLUDE_PROFILES, scanExcludeProfiles.orNull)
    return cp
  }

  private fun addToPropertyMap(map: MutableMap<String, String>, key: String, value: Boolean?) {
    if (value != null) {
      map[key] = value.toString()
    }
  }

  private fun addToPropertyMap(map: MutableMap<String, String>, key: String, value: String?) {
    if (value != null) {
      map[key] = value
    }
  }

  private fun addToPropertyMap(
    map: MutableMap<String, String>,
    key: String,
    values: List<String>?
  ) {
    if (values != null && values.isNotEmpty()) {
      map[key] = java.lang.String.join(",", values)
    }
  }

  private fun clearOutput() {
    val outputDir = outputDirectory.get().asFile
    if (outputDir.exists()) {
      outputDir.deleteRecursively()
    }
  }

  @Throws(GradleException::class)
  private fun write(schema: OpenApiDocument) {
    try {
      val yaml = OpenApiSerializer.serialize(schema.get(), Format.YAML)
      val json = OpenApiSerializer.serialize(schema.get(), Format.JSON)
      val directory = outputDirectory.get().asFile.toPath()
      if (!Files.exists(directory)) {
        Files.createDirectories(directory)
      }
      writeSchemaFile(directory, "yaml", yaml.toByteArray())
      writeSchemaFile(directory, "json", json.toByteArray())
      logger.info("Wrote the schema files to {}", outputDirectory.get().asFile.absolutePath)
    } catch (e: IOException) {
      throw GradleException("Can't write the result", e)
    }
  }

  @Throws(IOException::class)
  private fun writeSchemaFile(directory: Path, type: String, contents: ByteArray) {
    val file: Path = Paths.get(directory.toString(), schemaFilename.get() + "." + type)
    if (!Files.exists(file.parent)) {
      Files.createDirectories(file.parent)
    }
    if (!Files.exists(file)) {
      Files.createFile(file)
    }
    Files.write(
      file,
      contents,
      StandardOpenOption.WRITE,
      StandardOpenOption.CREATE,
      StandardOpenOption.TRUNCATE_EXISTING
    )
    // TODO if (attachArtifacts) {
    // mavenProjectHelper.attachArtifact(mavenProject, type, schemaFilename, file.toFile())
    // }
  }

  private val META_INF_OPENAPI_YAML = "META-INF/openapi.yaml"
  private val WEB_INF_CLASSES_META_INF_OPENAPI_YAML = "WEB-INF/classes/META-INF/openapi.yaml"
  private val META_INF_OPENAPI_YML = "META-INF/openapi.yml"
  private val WEB_INF_CLASSES_META_INF_OPENAPI_YML = "WEB-INF/classes/META-INF/openapi.yml"
  private val META_INF_OPENAPI_JSON = "META-INF/openapi.json"
  private val WEB_INF_CLASSES_META_INF_OPENAPI_JSON = "WEB-INF/classes/META-INF/openapi.json"

  /**
   * Directory where to output the schemas. If no path is specified, the schema will be printed to
   * the log.
   */
  @OutputDirectory
  val outputDirectory =
    project.objects
      .directoryProperty()
      .convention(project.layout.buildDirectory.dir("generated/openapi"))

  @InputFile
  @PathSensitive(PathSensitivity.RELATIVE)
  @Optional
  val configProperties = project.objects.fileProperty().convention(ext.configProperties)

  @Input val schemaFilename = project.objects.property(String::class).convention(ext.schemaFilename)

  @Input
  val scanDependenciesDisable =
    project.objects.property(Boolean::class).convention(ext.scanDependenciesDisable)

  @Input
  @Optional
  val attachArtifacts = project.objects.property(Boolean::class).convention(ext.attachArtifacts)

  @Input
  @Optional
  val modelReader = project.objects.property(String::class).convention(ext.modelReader)

  @Input @Optional val filter = project.objects.property(String::class).convention(ext.filter)

  @Input
  @Optional
  val scanDisabled = project.objects.property(Boolean::class).convention(ext.scanDisabled)

  @Input
  @Optional
  val scanPackages = project.objects.listProperty(String::class).convention(ext.scanPackages)

  @Input
  @Optional
  val scanClasses = project.objects.listProperty(String::class).convention(ext.scanClasses)

  @Input
  @Optional
  val scanExcludePackages =
    project.objects.listProperty(String::class).convention(ext.scanExcludePackages)

  @Input
  @Optional
  val scanExcludeClasses =
    project.objects.listProperty(String::class).convention(ext.scanExcludeClasses)

  @Input @Optional val servers = project.objects.listProperty(String::class).convention(ext.servers)

  @Input
  @Optional
  val pathServers = project.objects.listProperty(String::class).convention(ext.pathServers)

  @Input
  @Optional
  val operationServers =
    project.objects.listProperty(String::class).convention(ext.operationServers)

  @Input
  @Optional
  val customSchemaRegistryClass =
    project.objects.property(String::class).convention(ext.customSchemaRegistryClass)

  @Input
  val applicationPathDisable =
    project.objects.property(Boolean::class).convention(ext.applicationPathDisable)

  @Input val openApiVersion = project.objects.property(String::class).convention(ext.openApiVersion)

  @Input @Optional val infoTitle = project.objects.property(String::class).convention(ext.infoTitle)

  @Input
  @Optional
  val infoVersion = project.objects.property(String::class).convention(ext.infoVersion)

  @Input
  @Optional
  val infoDescription = project.objects.property(String::class).convention(ext.infoDescription)

  @Input
  @Optional
  val infoTermsOfService =
    project.objects.property(String::class).convention(ext.infoTermsOfService)

  @Input
  @Optional
  val infoContactEmail = project.objects.property(String::class).convention(ext.infoContactEmail)

  @Input
  @Optional
  val infoContactName = project.objects.property(String::class).convention(ext.infoContactName)

  @Input
  @Optional
  val infoContactUrl = project.objects.property(String::class).convention(ext.infoContactUrl)

  @Input
  @Optional
  val infoLicenseName = project.objects.property(String::class).convention(ext.infoLicenseName)

  @Input
  @Optional
  val infoLicenseUrl = project.objects.property(String::class).convention(ext.infoLicenseUrl)

  @Input
  @Optional
  val operationIdStrategy =
    project.objects.property(String::class).convention(ext.operationIdStrategy)

  @Input
  @Optional
  val scanProfiles = project.objects.listProperty(String::class).convention(ext.scanProfiles)

  @Input
  @Optional
  val scanExcludeProfiles =
    project.objects.listProperty(String::class).convention(ext.scanExcludeProfiles)
}

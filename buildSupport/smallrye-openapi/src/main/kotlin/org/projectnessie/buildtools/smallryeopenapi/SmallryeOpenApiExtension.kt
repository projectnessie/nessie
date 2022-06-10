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

import io.smallrye.openapi.api.constants.OpenApiConstants
import org.gradle.api.Project
import org.gradle.kotlin.dsl.listProperty
import org.gradle.kotlin.dsl.property

open class SmallryeOpenApiExtension(project: Project) {

  /**
   * Load any properties from a file. This file is loaded first, and gets overwritten by explicitly
   * set properties in the maven configuration. Example
   * `${basedir}/src/main/resources/application.properties`.
   */
  val configProperties = project.objects.fileProperty()

  /**
   * Filename of the schema Defaults to openapi. So the files created will be openapi.yaml and
   * openapi.json.
   */
  val schemaFilename = project.objects.property(String::class).convention("openapi")

  /** Disable scanning the project's dependencies for OpenAPI model classes too */
  val scanDependenciesDisable = project.objects.property(Boolean::class).convention(false)

  /** Attach the built OpenAPI schema as build artifact. */
  val attachArtifacts = project.objects.property(Boolean::class)

  /**
   * Configuration property to specify the fully qualified name of the OASModelReader
   * implementation.
   */
  val modelReader = project.objects.property(String::class)

  /** Configuration property to specify the fully qualified name of the OASFilter implementation. */
  val filter = project.objects.property(String::class)

  /** Configuration property to disable annotation scanning. */
  val scanDisabled = project.objects.property(Boolean::class)

  /** Configuration property to specify the list of packages to scan. */
  val scanPackages = project.objects.listProperty(String::class)

  /** Configuration property to specify the list of classes to scan. */
  val scanClasses = project.objects.listProperty(String::class)

  /** Configuration property to specify the list of packages to exclude from scans. */
  val scanExcludePackages = project.objects.listProperty(String::class)

  /** Configuration property to specify the list of classes to exclude from scans. */
  val scanExcludeClasses = project.objects.listProperty(String::class)

  /**
   * Configuration property to specify the list of global servers that provide connectivity
   * information.
   */
  val servers = project.objects.listProperty(String::class)

  /**
   * Prefix of the configuration property to specify an alternative list of servers to service all
   * operations in a path
   */
  val pathServers = project.objects.listProperty(String::class)

  /**
   * Prefix of the configuration property to specify an alternative list of servers to service an
   * operation.
   */
  val operationServers = project.objects.listProperty(String::class)

  /**
   * Fully qualified name of a CustomSchemaRegistry, which can be used to specify a custom schema
   * for a type.
   */
  val customSchemaRegistryClass = project.objects.property(String::class)

  /** Disable scanning of the javax.ws.rs.Path (and jakarta.ws.rs.Path) for the application path. */
  val applicationPathDisable = project.objects.property(Boolean::class).convention(false)

  /** To specify a custom OpenAPI version. */
  val openApiVersion =
    project.objects.property(String::class).convention(OpenApiConstants.OPEN_API_VERSION)

  val infoTitle = project.objects.property(String::class)

  val infoVersion = project.objects.property(String::class)

  val infoDescription = project.objects.property(String::class)

  val infoTermsOfService = project.objects.property(String::class)

  val infoContactEmail = project.objects.property(String::class)

  val infoContactName = project.objects.property(String::class)

  val infoContactUrl = project.objects.property(String::class)

  val infoLicenseName = project.objects.property(String::class)

  val infoLicenseUrl = project.objects.property(String::class)

  /**
   * Configuration property to specify how the operationid is generated. Can be used to minimize
   * risk of collisions between operations.
   */
  val operationIdStrategy = project.objects.property(String::class)

  /**
   * Profiles which explicitly include operations. Any operation without a matching profile is
   * excluded.
   */
  val scanProfiles = project.objects.listProperty(String::class)

  /**
   * Profiles which explicitly exclude operations. Any operation without a matching profile is
   * included.
   */
  val scanExcludeProfiles = project.objects.listProperty(String::class)
}

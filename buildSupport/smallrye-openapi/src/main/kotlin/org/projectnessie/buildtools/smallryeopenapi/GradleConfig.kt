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
import io.smallrye.openapi.api.constants.OpenApiConstants
import java.util.regex.Pattern
import org.eclipse.microprofile.openapi.OASConfig

class GradleConfig(val properties: Map<String, String>) : OpenApiConfig {

  override fun modelReader(): String? {
    return properties[OASConfig.MODEL_READER]
  }

  override fun filter(): String? {
    return properties[OASConfig.FILTER]
  }

  override fun scanDisable(): Boolean {
    return java.lang.Boolean.parseBoolean(properties[OASConfig.FILTER])
  }

  override fun scanPackages(): Pattern? {
    return patternOf(properties[OASConfig.SCAN_PACKAGES])
  }

  override fun scanClasses(): Pattern? {
    return patternOf(properties[OASConfig.SCAN_CLASSES])
  }

  override fun scanExcludePackages(): Pattern? {
    return patternOf(properties[OASConfig.SCAN_EXCLUDE_PACKAGES])
  }

  override fun scanExcludeClasses(): Pattern? {
    return patternOf(properties[OASConfig.SCAN_EXCLUDE_CLASSES])
  }

  override fun servers(): Set<String?>? {
    return asCsvSet(properties[OASConfig.SERVERS])
  }

  override fun pathServers(path: String): Set<String?>? {
    return asCsvSet(properties[OASConfig.SERVERS_PATH_PREFIX + path])
  }

  override fun operationServers(operationId: String): Set<String?>? {
    return asCsvSet(properties[OASConfig.SERVERS_OPERATION_PREFIX + operationId])
  }

  override fun scanDependenciesDisable(): Boolean {
    return java.lang.Boolean.parseBoolean(
      properties[OpenApiConstants.SMALLRYE_SCAN_DEPENDENCIES_DISABLE]
    )
  }

  override fun customSchemaRegistryClass(): String? {
    return properties[OpenApiConstants.SMALLRYE_CUSTOM_SCHEMA_REGISTRY_CLASS]
  }

  override fun applicationPathDisable(): Boolean {
    return java.lang.Boolean.parseBoolean(properties[OpenApiConstants.SMALLRYE_APP_PATH_DISABLE])
  }

  override fun getOpenApiVersion(): String? {
    return properties[OpenApiConstants.VERSION]
  }

  override fun getInfoTitle(): String? {
    return properties[OpenApiConstants.INFO_TITLE]
  }

  override fun getInfoVersion(): String? {
    return properties[OpenApiConstants.INFO_VERSION]
  }

  override fun getInfoDescription(): String? {
    return properties[OpenApiConstants.INFO_DESCRIPTION]
  }

  override fun getInfoTermsOfService(): String? {
    return properties[OpenApiConstants.INFO_TERMS]
  }

  override fun getInfoContactEmail(): String? {
    return properties[OpenApiConstants.INFO_CONTACT_EMAIL]
  }

  override fun getInfoContactName(): String? {
    return properties[OpenApiConstants.INFO_CONTACT_NAME]
  }

  override fun getInfoContactUrl(): String? {
    return properties[OpenApiConstants.INFO_CONTACT_URL]
  }

  override fun getInfoLicenseName(): String? {
    return properties[OpenApiConstants.INFO_LICENSE_NAME]
  }

  override fun getInfoLicenseUrl(): String? {
    return properties[OpenApiConstants.INFO_LICENSE_URL]
  }

  override fun getOperationIdStrategy(): OpenApiConfig.OperationIdStrategy? {
    val strategy: String? = properties[OpenApiConstants.OPERATION_ID_STRAGEGY]
    return if (strategy != null) {
      OpenApiConfig.OperationIdStrategy.valueOf(strategy)
    } else null
  }

  override fun getScanProfiles(): Set<String?>? {
    return asCsvSet(properties[OpenApiConstants.SCAN_PROFILES])
  }

  override fun getScanExcludeProfiles(): Set<String?>? {
    return asCsvSet(properties[OpenApiConstants.SCAN_EXCLUDE_PROFILES])
  }
}

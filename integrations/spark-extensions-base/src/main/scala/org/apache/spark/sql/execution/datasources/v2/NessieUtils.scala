/*
 * Copyright (C) 2020 Dremio
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
package org.apache.spark.sql.execution.datasources.v2

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.catalog.CatalogPlugin
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.projectnessie.client.{NessieClientBuilder, NessieConfigConstants}
import org.projectnessie.client.api.{NessieApiV1, NessieApiV2}
import org.projectnessie.client.config.NessieClientConfigSource
import org.projectnessie.error.{
  NessieNotFoundException,
  NessieReferenceNotFoundException
}
import org.projectnessie.model.Reference.ReferenceType
import org.projectnessie.model._

import java.lang.Boolean.parseBoolean
import java.time.format.DateTimeParseException
import java.time.{Instant, ZonedDateTime}
import java.util.Collections
import scala.collection.JavaConverters._

object NessieUtils {

  val BRANCH: String = "Branch"
  val TAG: String = "Tag"

  def unquoteRefName(branch: String): String = if (
    branch.startsWith("`") && branch.endsWith("`")
  ) {
    branch.substring(1, branch.length - 1)
  } else branch

  def calculateRef(
      branch: String,
      tsOrHash: Option[String],
      api: NessieApiV1
  ): Reference = {
    val refName = unquoteRefName(branch)

    val hash = tsOrHash
      .map(x => x.replaceAll("`", ""))
      .filter(x => Validation.isValidHash(x))
      .orNull

    if (null != hash) {
      return findReferenceFromHash(refName, hash, api)
    }

    val timestamp = tsOrHash
      .map(x => x.replaceAll("`", ""))
      .map(x => {
        try {
          ZonedDateTime.parse(x).toInstant
        } catch {
          case e: DateTimeParseException =>
            throw new NessieReferenceNotFoundException(
              String.format(
                "Invalid timestamp provided: %s. You need to provide it with a zone info. For more info, see: https://docs.oracle.com/javase/8/docs/api/java/time/format/DateTimeFormatter.html",
                e.getMessage
              )
            )
        }
      })
      .orNull

    if (timestamp == null) {
      api.getReference.refName(refName).get
    } else {
      findReferenceFromTimestamp(refName, api, timestamp)
    }
  }

  private def findReferenceFromHash(
      branch: String,
      requestedHash: String,
      api: NessieApiV1
  ) = {
    val commit = Option(
      api.getCommitLog
        .refName(branch)
        .hashOnRef(Validation.validateHash(requestedHash))
        .stream()
        .findFirst()
        .orElse(null)
    ).map(x => x.getCommitMeta.getHash)
    val hash = commit match {
      case Some(value) => value
      case None =>
        throw new NessieReferenceNotFoundException(
          String.format(
            "Cannot find requested hash %s on reference %s.",
            requestedHash,
            branch
          )
        )
    }
    convertToSpecificRef(
      hash,
      api.getReference.refName(branch).get()
    )
  }

  private def findReferenceFromTimestamp(
      branch: String,
      api: NessieApiV1,
      timestamp: Instant
  ) = {
    val commit = Option(
      api.getCommitLog
        .refName(branch)
        .filter(
          String.format(
            "timestamp(commit.commitTime) <= timestamp('%s')",
            timestamp
          )
        )
        .stream()
        .findFirst()
        .orElse(null)
    ).map(x => x.getCommitMeta.getHash)

    val hash = commit match {
      case Some(value) => value
      case None =>
        throw new NessieReferenceNotFoundException(
          String.format("Cannot find a hash before %s.", timestamp)
        )
    }
    convertToSpecificRef(
      hash,
      api.getReference.refName(branch).get()
    )
  }

  private def convertToSpecificRef(hash: String, reference: Reference) = {
    reference match {
      case branch: ImmutableBranch => Branch.of(branch.getName, hash)
      case tag: ImmutableTag       => Tag.of(tag.getName, hash)
      case _ =>
        throw new UnsupportedOperationException(
          s"Unknown reference type $reference"
        )
    }
  }

  def nessieAPI(
      currentCatalog: CatalogPlugin,
      catalog: Option[String]
  ): NessieApiV1 = {
    val maybeIcebergCatalog = getBaseIcebergCatalog(currentCatalog, catalog)
    val errorPre =
      "The command works only when the catalog is a NessieCatalog or a RESTCatalog using the Nessie Catalog Server"
    val errorPost =
      "Either set the catalog via USE <catalog_name> or provide the catalog during execution: <command> IN <catalog_name>."
    require(maybeIcebergCatalog.isDefined, errorPre + ". " + errorPost)

    val icebergCatalog = maybeIcebergCatalog.get

    val catalogName = catalog.getOrElse(currentCatalog.name)

    val nessieClientConfigMapper: NessieClientConfigSource =
      icebergCatalog.getClass.getSimpleName match {
        case "NessieCatalog" =>
          val sparkConf = SparkSession.active.sparkContext.conf
          val catalogConf = sparkConf
            .getAllWithPrefix(s"spark.sql.catalog.$catalogName.")
            .toMap
          x => catalogConf.getOrElse(x.replace("nessie.", ""), null)
        case "RESTCatalog" =>
          val catalogProperties = getCatalogProperties(icebergCatalog)

          require(
            parseBoolean(catalogProperties.get("nessie.is-nessie-catalog")),
            errorPre + ", but the referenced REST endpoint is not a Nessie Catalog Server. " + errorPost
          )

          // See o.a.i.rest.auth.OAuth2Properties.CREDENTIAL
          val credential = resolveCredential(catalogProperties)

          x => {
            x match {
              case NessieConfigConstants.CONF_NESSIE_URI =>
                // Use the Nessie Core REST API URL provided by Nessie Catalog Server. The Nessie Catalog
                // Server provides a _base_ URI without the `v1` or `v2` suffixes. We can safely assume
                // that `nessie.core-base-uri` contains a `/` terminated URI.
                catalogProperties.get("nessie.core-base-uri") + "v2"
              case NessieConfigConstants.CONF_NESSIE_OAUTH2_CLIENT_ID =>
                credential.clientId
              case NessieConfigConstants.CONF_NESSIE_OAUTH2_CLIENT_SECRET =>
                // See o.a.i.rest.auth.OAuth2Properties.CREDENTIAL
                credential.secret
              case NessieConfigConstants.CONF_NESSIE_OAUTH2_CLIENT_SCOPES =>
                // Same default scope as the Iceberg REST Client uses in o.a.i.rest.RESTSessionCatalog.initialize
                // See o.a.i.rest.auth.OAuth2Util.SCOPE
                resolveOAuthScope(catalogProperties)
              // TODO need the "token" (initial bearer token for OAuth2 as in o.a.i.rest.RESTSessionCatalog.initialize?
              case _ =>
                if (catalogProperties.containsKey(x)) catalogProperties.get(x)
                else catalogProperties.get(x.replace("nessie.", ""))
            }
          }
        case _ =>
          throw new IllegalArgumentException(
            errorPre + ", " + s"but $catalogName is a ${icebergCatalog.getClass.getName}. " + errorPost
          )
      }

    val nessieClientBuilder =
      NessieClientBuilder.createClientBuilderFromSystemSettings(
        nessieClientConfigMapper
      )
    nessieClientConfigMapper.getValue("nessie.client-api-version") match {
      case null | "1" =>
        nessieClientBuilder.build(classOf[NessieApiV1])
      case "2" =>
        nessieClientBuilder.build(classOf[NessieApiV2])
      case unsupported =>
        throw new IllegalArgumentException(
          String.format(
            "Unsupported client-api-version value: %s. Can only be 1 or 2",
            unsupported
          )
        )
    }
  }

  /** Allow resolving a property via the environment.
    */
  private def resolveViaEnvironment(
      properties: java.util.Map[String, String],
      property: String,
      defaultValue: String = null
  ): String = {
    val value = properties.get(property)
    if (value == null) {
      return defaultValue;
    }
    if (value.startsWith("env:")) {
      val env = System.getenv(value.substring("env:".length))
      if (env == null) {
        return defaultValue
      }
      env
    } else {
      value
    }
  }

  private def resolveOAuthScope(
      catalogProperties: java.util.Map[String, String]
  ): String = {
    val nessieScope = resolveViaEnvironment(
      catalogProperties,
      NessieConfigConstants.CONF_NESSIE_OAUTH2_CLIENT_SCOPES
    )
    if (nessieScope != null) {
      nessieScope
    } else {
      resolveViaEnvironment(catalogProperties, "scope", "catalog")
    }
  }

  private def resolveCredential(
      catalogProperties: java.util.Map[String, String]
  ): Credential = {
    val nessieClientId = resolveViaEnvironment(
      catalogProperties,
      NessieConfigConstants.CONF_NESSIE_OAUTH2_CLIENT_ID
    )
    val nessieClientSecret = resolveViaEnvironment(
      catalogProperties,
      NessieConfigConstants.CONF_NESSIE_OAUTH2_CLIENT_SECRET
    )

    val credentialFromIceberg = parseIcebergCredential(
      resolveViaEnvironment(catalogProperties, "credential")
    )

    Credential(
      if (nessieClientId != null) nessieClientId
      else credentialFromIceberg.clientId,
      if (nessieClientSecret != null) nessieClientSecret
      else credentialFromIceberg.secret
    )
  }

  private def parseIcebergCredential(credential: String): Credential = {
    // See o.a.i.rest.auth.OAuth2Util.parseCredential
    if (credential == null) {
      return Credential(null, null)
    }
    val colon = credential.indexOf(':')
    if (colon == -1) {
      Credential(null, credential)
    } else {
      Credential(
        credential.substring(0, colon),
        credential.substring(colon + 1)
      )
    }
  }

  /** @param currentCatalog
    *   The current Spark catalog
    * @param catalog
    *   The catalog to configure for Spark
    * @param ref
    *   The reference to configure for Spark
    * @param configureRefAtHash
    *   Whether to configure the ref at its given hash. Note that this should
    *   only be done when reading data from the given ref at its particular
    *   hash.
    */
  def setCurrentRefForSpark(
      currentCatalog: CatalogPlugin,
      catalog: Option[String],
      ref: Reference,
      configureRefAtHash: Boolean
  ): Unit = {
    val catalogName = catalog.getOrElse(currentCatalog.name)
    val catalogImpl =
      SparkSession.active.sessionState.catalogManager.catalog(catalogName)

    val activeConf = SparkSession.active.sparkContext.conf

    val icebergCatalog = getBaseIcebergCatalog(catalogImpl).get

    val confPrefix = s"spark.sql.catalog.$catalogName"

    icebergCatalog.getClass.getSimpleName match {
      case "NessieCatalog" =>
        activeConf.set(s"$confPrefix.ref", ref.getName)
        if (configureRefAtHash) {
          // we only configure ref.hash if we're reading data
          activeConf.set(
            s"$confPrefix.ref.hash",
            ref.getHash
          )
        } else {
          // we need to clear it in case it was previously set
          activeConf.remove(s"$confPrefix.ref.hash")
        }
      case "RESTCatalog" =>
        val catalogProperties = getCatalogProperties(icebergCatalog)
        if (parseBoolean(catalogProperties.get("nessie.is-nessie-catalog"))) {
          val nessiePrefixPattern =
            catalogProperties.get("nessie.prefix-pattern")

          val refAndWarehouse = refAndWarehouseFromPrefix(
            catalogProperties.get("prefix")
          )
          val warehouseSuffix =
            refAndWarehouse._2.map(w => s"|$w").getOrElse("")

          if (configureRefAtHash) {
            // we only configure ref.hash if we're reading data
            activeConf.set(
              s"$confPrefix.prefix",
              s"${ref.getName}@${ref.getHash}$warehouseSuffix"
            )
          } else {
            activeConf.set(
              s"$confPrefix.prefix",
              s"${ref.getName}$warehouseSuffix"
            )
          }
        }
    }

    val catalogConf = activeConf
      .getAllWithPrefix(s"$confPrefix.")
      .toMap
      .asJava
    if (icebergCatalog.isInstanceOf[AutoCloseable]) {
      icebergCatalog.asInstanceOf[AutoCloseable].close()
    }
    catalogImpl.initialize(
      catalogName,
      new CaseInsensitiveStringMap(catalogConf)
    )
  }

  def getBaseIcebergCatalog(
      currentCatalog: CatalogPlugin,
      catalog: Option[String]
  ): Option[Any] = {
    val catalogName = catalog.getOrElse(currentCatalog.name)
    val catalogImpl =
      SparkSession.active.sessionState.catalogManager.catalog(catalogName)

    getBaseIcebergCatalog(catalogImpl)
  }

  private def getBaseIcebergCatalog(
      catalogImpl: CatalogPlugin
  ): Option[Any] = {
    try {
      // `catalogImpl` is (should be) an org.apache.iceberg.spark.SparkCatalog.
      // We need the Iceberg `Catalog` instance from `SparkCatalog`
      var icebergCatalog = icebergCatalogFromSparkCatalog(catalogImpl)

      // If the Iceberg catalog is a `CachingCatalog`, get the "base" catalog from it.
      if (icebergCatalog.getClass.getSimpleName.equals("CachingCatalog")) {
        val cachingCatalogCatalog =
          icebergCatalog.getClass.getDeclaredField("catalog")
        cachingCatalogCatalog.setAccessible(true)
        icebergCatalog = cachingCatalogCatalog.get(icebergCatalog)
      }

      Some(icebergCatalog)
    } catch {
      // TODO have something better, at least log something
      case _: Exception => None
    }
  }

  private def icebergCatalogFromSparkCatalog(
      catalogImpl: CatalogPlugin
  ): Any = {
    // `catalogImpl` is an `org.apache.iceberg.spark.SparkCatalog`...
    try {
      // ... and most(!!) implementations of `o.a.i.s.SparkCatalog` have a
      // `public Catalog icebergCatalog()` function...
      catalogImpl.getClass
        .getDeclaredMethod("icebergCatalog")
        .invoke(catalogImpl)
    } catch {
      case _: NoSuchMethodException =>
        // ... but not *ALL* have that function, so we need to refer to the
        // field in these cases. :facepalm:
        val icebergCatalogField = catalogImpl.getClass
          .getDeclaredField("icebergCatalog")
        icebergCatalogField.setAccessible(true)
        icebergCatalogField.get(catalogImpl)
    }
  }

  private def getCatalogProperties(
      icebergCatalog: Any
  ): java.util.Map[String, String] = {
    try {
      // This is funny: all Iceberg catalogs have a function `Map<String, String> properties()`.
      // In `BaseMetastoreCatalog` it is protected, in `RESTCatalog`, which does not extend
      // `BaseMetastoreCatalog`, it is public.
      val catalogProperties =
        icebergCatalog.getClass.getDeclaredMethod("properties")
      catalogProperties.setAccessible(true)

      // Call the `properties()` function to get the catalog's configuration.
      val properties = catalogProperties
        .invoke(icebergCatalog)
        .asInstanceOf[java.util.Map[String, String]]

      properties
    } catch {
      // TODO have something better, at least log something
      case _: Exception => Collections.emptyMap()
    }
  }

  def getCurrentRef(
      api: NessieApiV1,
      currentCatalog: CatalogPlugin,
      catalog: Option[String]
  ): Reference = {
    val currentRef = getCurrentRef(currentCatalog, catalog)
    val refName = currentRef._1
    try {
      var ref = api.getReference.refName(refName).get
      val refHash = currentRef._2
      if (refHash.nonEmpty) {
        if (ref.getType == ReferenceType.BRANCH) {
          ref = Branch.of(ref.getName, refHash.get)
        } else {
          ref = Tag.of(ref.getName, refHash.get)
        }
      }
      ref
    } catch {
      case e: NessieNotFoundException =>
        throw new NessieReferenceNotFoundException(
          s"Could not find current reference $refName configured in spark configuration for catalog '${catalog
              .getOrElse(currentCatalog.name)}'.",
          e
        )
    }
  }

  def getCurrentRef(
      currentCatalog: CatalogPlugin,
      catalog: Option[String]
  ): (String, Option[String]) = {
    val catalogName = catalog.getOrElse(currentCatalog.name)
    val activeConf = SparkSession.active.sparkContext.conf
    val confPrefix = s"spark.sql.catalog.$catalogName"
    val icebergCatalog = getBaseIcebergCatalog(currentCatalog, catalog).get

    icebergCatalog.getClass.getSimpleName match {
      case "NessieCatalog" =>
        val refName = activeConf.get(s"$confPrefix.ref")
        var refHash: Option[String] = None
        try {
          refHash = Some(activeConf.get(s"$confPrefix.ref.hash"))
        } catch {
          case _: NoSuchElementException =>
        }
        (refName, refHash)
      case "RESTCatalog" =>
        val catalogProperties = getCatalogProperties(icebergCatalog)
        val prefix = catalogProperties.get("prefix")
        val refAndWarehouse = refAndWarehouseFromPrefix(prefix)
        refNameAndHash(refAndWarehouse._1)
    }
  }

  private def refAndWarehouseFromPrefix(
      prefix: String
  ): (String, Option[String]) = {
    val idx = prefix.indexOf("|")
    idx match {
      case -1 => (prefix, None)
      case _  => (prefix.substring(0, idx), Some(prefix.substring(idx + 1)))
    }
  }

  private def refNameAndHash(prefixRef: String): (String, Option[String]) = {
    val idx = prefixRef.indexOf("@")
    idx match {
      case -1 => (prefixRef, None)
      case _ =>
        (prefixRef.substring(0, idx), Some(prefixRef.substring(idx + 1)))
    }
  }

  def getRefType(ref: Reference): String = {
    ref match {
      case _: ImmutableBranch => NessieUtils.BRANCH
      case _: ImmutableTag    => NessieUtils.TAG
      case _ =>
        throw new UnsupportedOperationException(s"Unknown reference type $ref")
    }
  }

  case class Credential(clientId: String, secret: String)
}

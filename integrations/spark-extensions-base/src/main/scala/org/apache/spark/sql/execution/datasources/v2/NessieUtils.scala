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
import org.projectnessie.client.api.NessieApiV1
import org.projectnessie.client.http.HttpClientBuilder
import org.projectnessie.error.{
  NessieNotFoundException,
  NessieReferenceNotFoundException
}
import org.projectnessie.model.Reference.ReferenceType
import org.projectnessie.model.{
  Branch,
  ImmutableBranch,
  ImmutableTag,
  Reference,
  Tag,
  Validation
}

import java.time.format.DateTimeParseException
import java.time.{Instant, ZonedDateTime}
import scala.collection.JavaConverters._

object NessieUtils {

  val BRANCH: String = "Branch"
  val TAG: String = "Tag"
  val HASH: String = "Hash"

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
    val catalogName = catalog.getOrElse(currentCatalog.name)
    val sparkConf = SparkSession.active.sparkContext.conf
    val catalogConf = sparkConf
      .getAllWithPrefix(s"spark.sql.catalog.$catalogName.")
      .toMap

    val catalogImpl = catalogConf.get("catalog-impl")
    val catalogErrorDetail = catalogImpl match {
      case Some(clazz) => s"but $catalogName is a $clazz"
      case None =>
        s"but spark.sql.catalog.$catalogName.catalog-impl is not set"
    }
    // Referring to https://github.com/apache/iceberg/blob/master/nessie/src/main/java/org/apache/iceberg/nessie/NessieCatalog.java
    // Not using fully-qualified class name to provide protection from shading activities (if any)
    require(
      catalogImpl
        .exists(impl => impl.endsWith(".NessieCatalog")),
      s"The command works only when the catalog is a NessieCatalog ($catalogErrorDetail). Either set the catalog via USE <catalog_name> or provide the catalog during execution: <command> IN <catalog_name>."
    )

    HttpClientBuilder
      .builder()
      .fromConfig(x => catalogConf.getOrElse(x.replace("nessie.", ""), null))
      .build(classOf[NessieApiV1])
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
    SparkSession.active.sparkContext.conf
      .set(s"spark.sql.catalog.$catalogName.ref", ref.getName)
    if (configureRefAtHash) {
      // we only configure ref.hash if we're reading data
      SparkSession.active.sparkContext.conf
        .set(s"spark.sql.catalog.$catalogName.ref.hash", ref.getHash)
    } else {
      // we need to clear it in case it was previously set
      SparkSession.active.sparkContext.conf
        .remove(s"spark.sql.catalog.$catalogName.ref.hash")
    }
    val catalogConf = SparkSession.active.sparkContext.conf
      .getAllWithPrefix(s"spark.sql.catalog.$catalogName.")
      .toMap
      .asJava
    catalogImpl.initialize(
      catalogName,
      new CaseInsensitiveStringMap(catalogConf)
    )
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
    val refName = SparkSession.active.sparkContext.conf
      .get(s"spark.sql.catalog.$catalogName.ref")
    var refHash: Option[String] = None
    try {
      refHash = Some(
        SparkSession.active.sparkContext.conf
          .get(s"spark.sql.catalog.$catalogName.ref.hash")
      )
    } catch {
      case _: NoSuchElementException =>
    }
    (refName, refHash)
  }

  def getRefType(ref: Reference): String = {
    ref match {
      case _: ImmutableBranch => NessieUtils.BRANCH
      case _: ImmutableTag    => NessieUtils.TAG
      case _ =>
        throw new UnsupportedOperationException(s"Unknown reference type $ref")
    }
  }
}

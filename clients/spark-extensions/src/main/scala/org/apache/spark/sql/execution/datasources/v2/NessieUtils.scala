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

import java.time.format.DateTimeParseException
import java.time.{Instant, LocalDateTime, ZoneOffset}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.catalog.CatalogPlugin
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.projectnessie.api.params.CommitLogParams
import org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_URI
import org.projectnessie.client.{NessieClient, StreamingUtil}
import org.projectnessie.error.NessieNotFoundException
import org.projectnessie.model.{
  Branch,
  Hash,
  ImmutableBranch,
  ImmutableHash,
  ImmutableTag,
  Reference,
  Tag,
  Validation
}

import scala.collection.JavaConverters._

object NessieUtils {

  val BRANCH: String = "Branch"
  val TAG: String = "Tag"
  val HASH: String = "Hash"

  def calculateRef(
      branch: String,
      tsOrHash: Option[String],
      nessieClient: NessieClient
  ): Reference = {
    val hash = tsOrHash
      .map(x => x.replaceAll("`", ""))
      .filter(x => Validation.isValidHash(x))
      .orNull

    if (null != hash) {
      return findReferenceFromHash(branch, hash, nessieClient)
    }

    //todo we are assuming always in UTC. ignoring tz set by spark etc
    val timestamp = tsOrHash
      .map(x => x.replaceAll("`", ""))
      .map(x => {
        try {
          LocalDateTime.parse(x).atZone(ZoneOffset.UTC).toInstant
        } catch {
          case e: DateTimeParseException =>
            throw new NessieNotFoundException(
              "Invalid timestamp provided: " + e.getMessage
            )
        }
      })
      .orNull

    if (timestamp == null) {
      nessieClient.getTreeApi.getReferenceByName(branch)
    } else {
      findReferenceFromTimestamp(branch, nessieClient, timestamp)
    }
  }

  private def findReferenceFromHash(
      branch: String,
      requestedHash: String,
      nessieClient: NessieClient
  ) = {
    val commit = Option(
      StreamingUtil
        .getCommitLogStream(
          nessieClient.getTreeApi,
          branch,
          CommitLogParams
            .builder()
            .endHash(Validation.validateHash(requestedHash))
            .build()
        )
        .findFirst()
        .orElse(null)
    ).map(x => Hash.of(x.getHash))
    println(commit)
    val hash = commit match {
      case Some(value) => value
      case None =>
        throw new NessieNotFoundException(
          String.format(
            "Cannot find requested hash %s on reference %s.",
            requestedHash,
            branch
          )
        )
    }
    convertToSpecificRef(
      hash,
      nessieClient.getTreeApi.getReferenceByName(branch)
    )
  }

  private def findReferenceFromTimestamp(
      branch: String,
      nessieClient: NessieClient,
      timestamp: Instant
  ) = {
    val commit = Option(
      StreamingUtil
        .getCommitLogStream(
          nessieClient.getTreeApi,
          branch,
          CommitLogParams
            .builder()
            .expression(
              String.format(
                "timestamp(commit.commitTime) < timestamp('%s')",
                timestamp
              )
            )
            .build()
        )
        .findFirst()
        .orElse(null)
    ).map(x => Hash.of(x.getHash))

    val hash = commit match {
      case Some(value) => value
      case None =>
        throw new NessieNotFoundException(
          String.format("Cannot find a hash before %s.", timestamp)
        )
    }
    convertToSpecificRef(
      hash,
      nessieClient.getTreeApi.getReferenceByName(branch)
    )
  }

  private def convertToSpecificRef(hash: Hash, reference: Reference) = {
    reference match {
      case branch: ImmutableBranch => Branch.of(branch.getName, hash.getHash)
      case hash: ImmutableHash     => hash
      case tag: ImmutableTag       => Tag.of(tag.getName, hash.getHash)
      case _ =>
        throw new UnsupportedOperationException(
          s"Unknown reference type $reference"
        )
    }
  }

  def nessieClient(
      currentCatalog: CatalogPlugin,
      catalog: Option[String]
  ): NessieClient = {
    val catalogName = catalog.getOrElse(currentCatalog.name)
    val catalogConf = SparkSession.active.sparkContext.conf
      .getAllWithPrefix(s"spark.sql.catalog.$catalogName.")
      .toMap
    val uriConf = CONF_NESSIE_URI.replace("nessie.", "")
    require(
      catalogConf.get(uriConf).nonEmpty,
      s"Nessie catalog URI not defined. Please set a value for conf [spark.sql.catalog.$catalogName.$uriConf]."
    )
    NessieClient
      .builder()
      .fromConfig(x => catalogConf.getOrElse(x.replace("nessie.", ""), null))
      .build()
  }

  def setCurrentRefForSpark(
      currentCatalog: CatalogPlugin,
      catalog: Option[String],
      ref: Reference
  ): Unit = {
    val catalogName = catalog.getOrElse(currentCatalog.name)
    val catalogImpl =
      SparkSession.active.sessionState.catalogManager.catalog(catalogName)
    SparkSession.active.sparkContext.conf
      .set(s"spark.sql.catalog.$catalogName.ref", ref.getName)
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
      currentCatalog: CatalogPlugin,
      catalog: Option[String]
  ): Reference = {
    val catalogName = catalog.getOrElse(currentCatalog.name)
    val refName = SparkSession.active.sparkContext.conf
      .get(s"spark.sql.catalog.$catalogName.ref")
    nessieClient(currentCatalog, catalog).getTreeApi.getReferenceByName(refName)
  }

  def getRefType(ref: Reference): String = {
    ref match {
      case branch: ImmutableBranch => NessieUtils.BRANCH
      case hash: ImmutableHash     => NessieUtils.HASH
      case tag: ImmutableTag       => NessieUtils.TAG
      case _ =>
        throw new UnsupportedOperationException(s"Unknown reference type $ref")
    }
  }
}

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
import org.projectnessie.client.{NessieClient, StreamingUtil}
import org.projectnessie.error.NessieNotFoundException
import org.projectnessie.model.{Hash, Reference}

import java.time.{LocalDateTime, ZoneId, ZoneOffset}
import java.util.OptionalInt
import scala.collection.JavaConverters._

object NessieUtils {

  val BRANCH: String = "Branch"
  val TAG: String = "Tag"
  val HASH: String = "Hash"

  def calculateRef(
      branch: String,
      ts: Option[String],
      nessieClient: NessieClient
  ): Reference = {
    //todo we are assuming always in UTC. ignoring tz set by spark etc
    val timestamp = ts
      .map(x => x.replaceAll("`", ""))
      .map(x => LocalDateTime.parse(x).atZone(ZoneOffset.UTC).toInstant)
      .orNull
    if (timestamp == null) {
      nessieClient.getTreeApi.getReferenceByName(branch)
    } else {
      StreamingUtil
        .getCommitLogStream(
          nessieClient.getTreeApi,
          branch,
          OptionalInt.empty(),
          String
            .format("timestamp(commit.commitTime) < timestamp('%s')", timestamp)
        )
        .findFirst()
        .map(x => Hash.of(x.getHash))
        .orElseThrow(
          () =>
            new NessieNotFoundException(
              String.format("Cannot find a hash before %s.", timestamp)
            )
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
    NessieClient
      .builder()
      .fromConfig(x => catalogConf.getOrElse(x.replace("nessie.", ""), null))
      .build()
  }

  def setCurrentRef(
      currentCatalog: CatalogPlugin,
      catalog: Option[String],
      ref: Reference
  ): Reference = {
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
    getCurrentRef(currentCatalog, catalog)
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

}

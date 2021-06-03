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
import org.apache.spark.sql.connector.catalog.CatalogPlugin
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.projectnessie.api.params.CommitLogParams
import org.projectnessie.client.{NessieClient, StreamingUtil}
import org.projectnessie.model.{Branch, CommitMeta, Hash, Reference}

import java.time.{LocalDateTime, ZoneId}
import scala.collection.JavaConverters._

object NessieUtils {
  def calculateRef(
      branch: String,
      ts: Option[String],
      nessieClient: NessieClient
  ): Reference = {
    //todo we are assuming always in UTC. ignoring tz set by spark etc
    val timestamp = ts
      .map(x => x.replaceAll("`", ""))
      .map(x => LocalDateTime.parse(x).atZone(ZoneId.of("UTC")).toInstant)
      .orNull
    if (timestamp == null) {
      nessieClient.getTreeApi.getReferenceByName(branch)
    } else {
      var last: Option[CommitMeta] = None
      // todo this is icky
      for (cm <- StreamingUtil
             .getCommitLogStream(
               nessieClient.getTreeApi,
               branch,
               CommitLogParams.empty()
             )
             .iterator
             .asScala) {
        if (cm.getCommitTime.isBefore(timestamp)) {
          Hash.of(cm.getHash)
        }
        last = Some(cm)
      }
      last
        .map(x => Hash.of(x.getHash))
        .getOrElse(nessieClient.getTreeApi.getReferenceByName(branch))
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
//    if (isIcebergNessie(catalogImpl)) {
//      setIcebergRef(getIcebergSparkCatalog(catalogImpl), ref)
//    } else if (isDeltaNessie) {
//      // todo, how do we get current Ref?
//      null
//    } else {
//      throw new AnalysisException("Catalog is not Nessie enabled")
//    }
  }

  def getCurrentRef(
      currentCatalog: CatalogPlugin,
      catalog: Option[String]
  ): Reference = {
    val catalogName = catalog.getOrElse(currentCatalog.name)
    val refName = SparkSession.active.sparkContext.conf
      .get(s"spark.sql.catalog.$catalogName.ref")
    nessieClient(currentCatalog, catalog).getTreeApi.getReferenceByName(refName)
//    if (isIcebergNessie(catalogImpl)) {
//      getIcebergRef(getIcebergSparkCatalog(catalogImpl))
//    } else if (isDeltaNessie) {
//      // todo, how do we get current Ref?
//      null
//    } else {
//      throw new AnalysisException("Catalog is not Nessie enabled")
//    }
  }

  private def isDeltaNessie: Boolean = {
    SparkSession.active.sparkContext.conf
      .contains("spark.delta.logStore.class") &&
    SparkSession.active.sparkContext.conf
      .get("spark.delta.logStore.class")
      .contains("NessieLogStore")
  }

  private def isIcebergNessie(catalogImpl: CatalogPlugin): Boolean = {
    catalogImpl.getClass.getCanonicalName.equals(
      "org.apache.iceberg.spark.SparkCatalog"
    ) || catalogImpl.getClass.getCanonicalName
      .equals("org.apache.iceberg.spark.SparkSessionCatalog")
  }

  private def getIcebergSparkCatalog(
      catalogImpl: CatalogPlugin
  ): CatalogPlugin = {
    val className = catalogImpl.getClass.getCanonicalName
    className match {
      case "org.apache.iceberg.spark.SparkSessionCatalog" =>
        getIcebergCatalogFromSessionCatalog(catalogImpl)
      case "org.apache.iceberg.spark.SparkCatalog" => catalogImpl
      case _                                       => throw new AnalysisException("Unknown Iceberg Catalog")
    }
  }

  private def getIcebergRef(catalogImpl: CatalogPlugin): Reference = {
    val field = Class
      .forName("org.apache.iceberg.spark.SparkCatalog")
      .getDeclaredField("icebergCatalog")
    field.setAccessible(true)
    val catalog = unwrapCachingCatalog(field.get(catalogImpl))
    val updateRef = Class
      .forName("org.apache.iceberg.nessie.NessieCatalog")
      .getMethod("copyReference")
    //todo we need to update Nessie catalog to support these methods
    updateRef.invoke(catalog).asInstanceOf[Reference]
  }

  private def setIcebergRef(
      plugin: CatalogPlugin,
      reference: Reference
  ): Reference = {
    val field = Class
      .forName("org.apache.iceberg.spark.SparkCatalog")
      .getDeclaredField("icebergCatalog")
    field.setAccessible(true)
    val catalog = unwrapCachingCatalog(field.get(plugin))
    val updateRef = Class
      .forName("org.apache.iceberg.nessie.NessieCatalog")
      .getMethod(
        "updateReference",
        Class.forName("org.projectnessie.model.Reference")
      )
    updateRef.invoke(catalog, reference)
    getIcebergRef(plugin)
  }

  private def unwrapCachingCatalog(catalogImpl: AnyRef): AnyRef = {
    val className = catalogImpl.getClass.getCanonicalName
    className match {
      case "org.apache.iceberg.CachingCatalog" =>
        val field = Class
          .forName("org.apache.iceberg.CachingCatalog")
          .getDeclaredField("catalog")
        field.setAccessible(true)
        field.get(catalogImpl)
      case "org.apache.iceberg.nessie.NessieCatalog" => catalogImpl
      case _                                         => throw new AnalysisException("Unknown Iceberg Catalog")
    }
  }

  private def getIcebergCatalogFromSessionCatalog(
      catalogImpl: CatalogPlugin
  ): CatalogPlugin = {
    val field = Class
      .forName("org.apache.iceberg.spark.SparkSessionCatalog")
      .getDeclaredField("icebergCatalog")
    field.setAccessible(true)
    field.get(catalogImpl).asInstanceOf[CatalogPlugin]
  }
}

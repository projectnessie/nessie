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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, MapData}
import org.apache.spark.sql.connector.catalog.CatalogPlugin
import org.apache.spark.sql.execution.datasources.v2.NessieUtils.unquoteRefName
import org.apache.spark.unsafe.types.UTF8String

import java.time.Instant
import java.time.temporal.ChronoUnit

import scala.jdk.CollectionConverters._

case class ShowLogExec(
    output: Seq[Attribute],
    branch: Option[String],
    timestampOrHash: Option[String],
    limit: Option[Int],
    currentCatalog: CatalogPlugin,
    catalog: Option[String]
) extends NessieExec(catalog = catalog, currentCatalog = currentCatalog)
    with LeafV2CommandExec {

  override protected def runInternal(
      bridge: CatalogBridge
  ): Seq[InternalRow] = {

    val refName = branch
      .map(unquoteRefName)
      .getOrElse(
        bridge.getCurrentRef.getName
      )

    val ref = NessieUtils.calculateRef(refName, timestampOrHash, bridge.api)

    var stream = bridge.api.getCommitLog.reference(ref).stream()

    if (limit.isDefined) {
      stream = stream.limit(limit.get)
    }

    stream.iterator.asScala
      .map(entry =>
        InternalRow(
          convert(entry.getCommitMeta.getAuthor),
          convert(entry.getCommitMeta.getCommitter),
          convert(entry.getCommitMeta.getHash),
          convert(entry.getCommitMeta.getMessage),
          convert(entry.getCommitMeta.getSignedOffBy),
          convert(entry.getCommitMeta.getAuthorTime),
          convert(entry.getCommitMeta.getCommitTime),
          convert(entry.getCommitMeta.getProperties)
        )
      )
      .toSeq
  }

  override def simpleString(maxFields: Int): String = {
    s"ShowLogExec ${catalog.getOrElse(currentCatalog.name())} ${branch.map(unquoteRefName)} "
  }

  private def convert(input: String): UTF8String = {
    UTF8String.fromString(if (input == null) "" else input)
  }

  private def convert(input: Instant): Long = {
    ChronoUnit.MICROS.between(Instant.EPOCH, input)
  }

  private def convert(input: java.util.Map[String, String]): MapData = {
    ArrayBasedMapData(input, x => convertMapKV(x), x => convertMapKV(x))
  }

  private def convertMapKV(input: Any): Any = {
    input match {
      case c: String  => convert(c)
      case c: Instant => convert(c)
      case c          => c
    }
  }
}

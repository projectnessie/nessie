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
import org.apache.spark.unsafe.types.UTF8String
import org.projectnessie.api.params.CommitLogParams
import org.projectnessie.client.{NessieClient, StreamingUtil}

import java.time.Instant
import java.time.temporal.ChronoUnit
import scala.collection.JavaConverters._

case class ShowLogExec(
    output: Seq[Attribute],
    branch: Option[String],
    currentCatalog: CatalogPlugin,
    catalog: Option[String]
) extends V2CommandExec {

  lazy val nessieClient: NessieClient =
    NessieUtils.nessieClient(currentCatalog, catalog)

  override protected def run(): Seq[InternalRow] = {
    val refName = branch.getOrElse(
      NessieUtils.getCurrentRef(currentCatalog, catalog).getName
    )
    val stream = StreamingUtil.getCommitLogStream(
      nessieClient.getTreeApi,
      refName,
      CommitLogParams.empty()
    )

    stream.iterator.asScala
      .map(
        cm =>
          InternalRow(
            convert(cm.getAuthor),
            convert(cm.getCommitter),
            convert(cm.getHash),
            convert(cm.getMessage),
            convert(cm.getSignedOffBy),
            convert(cm.getAuthorTime),
            convert(cm.getCommitTime),
            convert(cm.getProperties)
          )
      )
      .toSeq
  }

  override def simpleString(maxFields: Int): String = {
    s"ShowLogExec ${catalog.getOrElse(currentCatalog.name())} ${branch} "
  }

  private def convert(input: String): UTF8String = {
    UTF8String.fromString(if (input == null) "" else input)
  }

  private def convert(input: Instant): Long = {
    ChronoUnit.MICROS.between(Instant.EPOCH, input)
  }

  private def convert(input: java.util.Map[String, String]): MapData = {
    ArrayBasedMapData(input, x => x, x => x)
  }
}

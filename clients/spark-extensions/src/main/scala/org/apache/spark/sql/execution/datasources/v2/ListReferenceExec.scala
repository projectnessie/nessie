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
import org.apache.spark.sql.connector.catalog.CatalogPlugin
import org.apache.spark.unsafe.types.UTF8String
import org.projectnessie.client.NessieClient
import org.projectnessie.model.Branch

import scala.collection.JavaConverters._

case class ListReferenceExec(
    output: Seq[Attribute],
    currentCatalog: CatalogPlugin,
    catalog: Option[String]
) extends NessieExec(catalog = catalog, currentCatalog = currentCatalog) {

  override protected def runInternal(
      nessieClient: NessieClient
  ): Seq[InternalRow] = {
    nessieClient.getTreeApi.getAllReferences.asScala.map(ref => {
      InternalRow(
        UTF8String.fromString(NessieUtils.getRefType(ref)),
        UTF8String.fromString(ref.getName),
        UTF8String.fromString(ref.getHash)
      )
    })
  }

  override def simpleString(maxFields: Int): String = {
    s"ListReferenceExec ${catalog.getOrElse(currentCatalog.name())} "
  }
}

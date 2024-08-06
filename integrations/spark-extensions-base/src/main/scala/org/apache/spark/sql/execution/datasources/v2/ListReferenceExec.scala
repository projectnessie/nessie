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
import org.projectnessie.client.api.GetAllReferencesBuilder
import org.projectnessie.model.FetchOption

import scala.jdk.CollectionConverters._

case class ListReferenceExec(
    output: Seq[Attribute],
    currentCatalog: CatalogPlugin,
    filter: Option[String],
    startsWith: Option[String],
    contains: Option[String],
    catalog: Option[String]
) extends NessieExec(catalog = catalog, currentCatalog = currentCatalog)
    with LeafV2CommandExec {

  override protected def runInternal(
      bridge: CatalogBridge
  ): Seq[InternalRow] = {
    val fetchOption = FetchOption.MINIMAL;

    var filterStr: Option[String] = None
    if (filter.isDefined) {
      filterStr = filter
    } else {
      if (startsWith.isDefined) {
        filterStr = Some(s"ref.name.startsWith('${startsWith.get}')")
      }
      if (contains.isDefined) {
        val f = s"ref.name.contains('${contains.get}')"
        filterStr =
          filterStr.map(f => s"${filterStr.get} && $f").orElse(Some(f))
      }
    }

    val refs = bridge.api.getAllReferences.fetch(fetchOption)
    filterStr.foreach(f => refs.filter(f))
    refs
      .stream()
      .iterator()
      .asScala
      .map(ref => rowForRef(ref))
      .toSeq // required for Scala 2.13
  }

  override def simpleString(maxFields: Int): String = {
    s"ListReferenceExec ${catalog.getOrElse(currentCatalog.name())} "
  }
}

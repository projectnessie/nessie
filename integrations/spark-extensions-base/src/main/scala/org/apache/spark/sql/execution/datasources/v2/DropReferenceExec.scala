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
import org.apache.spark.sql.execution.datasources.v2.NessieUtils.unquoteRefName
import org.apache.spark.unsafe.types.UTF8String
import org.projectnessie.error.NessieReferenceNotFoundException

case class DropReferenceExec(
    output: Seq[Attribute],
    branch: String,
    currentCatalog: CatalogPlugin,
    isBranch: Boolean,
    catalog: Option[String],
    failOnDrop: Boolean
) extends NessieExec(catalog = catalog, currentCatalog = currentCatalog)
    with LeafV2CommandExec {

  override protected def runInternal(
      bridge: CatalogBridge
  ): Seq[InternalRow] = {
    val refName = unquoteRefName(branch)
    try {
      val hash = bridge.api.getReference.refName(refName).get().getHash
      if (isBranch) {
        bridge.api.deleteBranch().branchName(refName).hash(hash).delete()
      } else {
        bridge.api.deleteTag().tagName(refName).hash(hash).delete()
      }
    } catch {
      case e: NessieReferenceNotFoundException =>
        if (failOnDrop) {
          throw e
        }
    }
    Seq(InternalRow(UTF8String.fromString("OK")))
  }

  override def simpleString(maxFields: Int): String = {
    s"DropReferenceExec ${catalog.getOrElse(currentCatalog.name())} ${if (isBranch) "BRANCH"
      else "TAG"} ${unquoteRefName(branch)} "
  }

}

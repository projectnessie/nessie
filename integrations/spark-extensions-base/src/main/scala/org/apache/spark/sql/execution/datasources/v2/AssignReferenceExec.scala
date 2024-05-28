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
import org.projectnessie.model.{Branch, Tag}

case class AssignReferenceExec(
    output: Seq[Attribute],
    refNameToAssign: String,
    isBranch: Boolean,
    currentCatalog: CatalogPlugin,
    toRefName: Option[String],
    toHash: Option[String],
    catalog: Option[String]
) extends NessieExec(catalog = catalog, currentCatalog = currentCatalog)
    with LeafV2CommandExec {

  override protected def runInternal(
      bridge: CatalogBridge
  ): Seq[InternalRow] = {
    val refNameAssign = unquoteRefName(refNameToAssign)
    val currentHash =
      bridge.api.getReference.refName(refNameAssign).get().getHash

    val toRef =
      if (toRefName.isDefined)
        bridge.api.getReference.refName(toRefName.map(unquoteRefName).get).get()
      else bridge.getCurrentRef

    val assignToHash = toHash.getOrElse(toRef.getHash)
    val assignTo =
      if (toRef.isInstanceOf[Branch])
        Branch.of(toRef.getName, assignToHash)
      else Tag.of(toRef.getName, assignToHash)

    if (isBranch) {
      bridge.api
        .assignBranch()
        .branch(Branch.of(refNameAssign, currentHash))
        .assignTo(assignTo)
        .assign()
    } else {
      bridge.api
        .assignTag()
        .tag(Tag.of(refNameAssign, currentHash))
        .assignTo(assignTo)
        .assign()
    }
    val ref = bridge.api.getReference.refName(refNameAssign).get()

    singleRowForRef(ref)
  }

  override def simpleString(maxFields: Int): String = {
    s"AssignReferenceExec ${catalog.getOrElse(currentCatalog.name())} ${unquoteRefName(refNameToAssign)} "
  }
}

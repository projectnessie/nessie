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
import org.projectnessie.error.{
  NessieConflictException,
  NessieNotFoundException,
  NessieReferenceNotFoundException
}
import org.projectnessie.model.{Branch, Tag}

case class CreateReferenceExec(
    output: Seq[Attribute],
    ref: String,
    refTimestampOrHash: Option[String],
    currentCatalog: CatalogPlugin,
    isBranch: Boolean,
    catalog: Option[String],
    createdFrom: Option[String],
    failOnCreate: Boolean
) extends NessieExec(catalog = catalog, currentCatalog = currentCatalog)
    with LeafV2CommandExec {

  override protected def runInternal(
      bridge: CatalogBridge
  ): Seq[InternalRow] = {
    val sourceRef =
      if (createdFrom.isDefined) {
        bridge.api.getReference
          .refName(createdFrom.map(unquoteRefName).get)
          .get()
      } else {
        try {
          bridge.getCurrentRef
        } catch {
          case e: NessieNotFoundException =>
            throw new NessieReferenceNotFoundException(
              s"${e.getMessage} Use 'CREATE ${if (isBranch) "BRANCH" else "TAG"} ... FROM <existing-reference-name>'.",
              e
            )
        }
      }
    val refName = unquoteRefName(ref)
    val refObj = {
      if (isBranch) Branch.of(refName, sourceRef.getHash)
      else Tag.of(refName, sourceRef.getHash)
    }
    val result =
      try {
        bridge.api.createReference
          .reference(refObj)
          .sourceRefName(sourceRef.getName)
          .create()
      } catch {
        case e: NessieConflictException =>
          if (failOnCreate) {
            throw e
          }
          bridge.api.getReference.refName(refObj.getName).get()
      }

    singleRowForRef(result)
  }

  override def simpleString(maxFields: Int): String = {
    s"CreateReferenceExec ${catalog.getOrElse(currentCatalog.name())} ${if (isBranch) "BRANCH"
      else "TAG"} ${unquoteRefName(ref)} " +
      s"${createdFrom.map(unquoteRefName)}"
  }

}

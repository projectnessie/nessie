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
import org.projectnessie.model.{
  Branch,
  ImmutableBranch,
  ImmutableHash,
  ImmutableMerge,
  ImmutableTag,
  Tag
}

case class AssignReferenceExec(
    output: Seq[Attribute],
    branch: String,
    isBranch: Boolean,
    currentCatalog: CatalogPlugin,
    toRefName: Option[String],
    catalog: Option[String]
) extends NessieExec(catalog = catalog, currentCatalog = currentCatalog) {

  override protected def runInternal(
      nessieClient: NessieClient
  ): Seq[InternalRow] = {
    val toHash = toRefName
      .map(r => nessieClient.getTreeApi.getReferenceByName(r).getHash)
      .getOrElse(nessieClient.getTreeApi.getDefaultBranch.getHash)
    val hash = nessieClient.getTreeApi.getReferenceByName(branch).getHash
    if (isBranch) {
      nessieClient.getTreeApi.assignBranch(
        branch,
        hash,
        Branch.of(branch, toHash)
      )
    } else {
      nessieClient.getTreeApi.assignTag(branch, hash, Tag.of(branch, toHash))
    }

    val ref = nessieClient.getTreeApi.getReferenceByName(branch)

    Seq(
      InternalRow(
        UTF8String.fromString(NessieUtils.getRefType(ref)),
        UTF8String.fromString(ref.getName),
        UTF8String.fromString(ref.getHash)
      )
    )
  }

  override def simpleString(maxFields: Int): String = {
    s"AssignReferenceExec ${catalog.getOrElse(currentCatalog.name())} ${branch} "
  }
}

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
import org.projectnessie.error.NessieConflictException
import org.projectnessie.model._

case class CreateReferenceExec(
    output: Seq[Attribute],
    branch: String,
    currentCatalog: CatalogPlugin,
    isBranch: Boolean,
    catalog: Option[String],
    createdFrom: Option[String],
    failOnCreate: Boolean
) extends NessieExec(catalog = catalog, currentCatalog = currentCatalog) {

  override protected def runInternal(
      nessieClient: NessieClient
  ): Seq[InternalRow] = {
    val hash = createdFrom
      .map(nessieClient.getTreeApi.getReferenceByName)
      .orElse(Option(nessieClient.getTreeApi.getDefaultBranch))
      .map(x => x.getHash)
      .orNull
    val ref = if (isBranch) Branch.of(branch, hash) else Tag.of(branch, hash)
    try {
      nessieClient.getTreeApi.createReference(ref)
    } catch {
      case e: NessieConflictException =>
        if (failOnCreate) {
          throw e
        }
    }
    val branchResult = nessieClient.getTreeApi.getReferenceByName(ref.getName)
    val refType = branchResult match {
      case _: ImmutableHash   => NessieUtils.HASH
      case _: ImmutableBranch => NessieUtils.BRANCH
      case _: ImmutableTag    => NessieUtils.TAG
    }
    Seq(
      InternalRow(
        UTF8String.fromString(refType),
        UTF8String.fromString(branchResult.getName),
        UTF8String.fromString(branchResult.getHash)
      )
    )
  }

  override def simpleString(maxFields: Int): String = {
    s"CreateReferenceExec ${catalog.getOrElse(currentCatalog.name())} ${if (isBranch) "BRANCH"
    else "TAG"} ${branch} " +
      s"${createdFrom}"
  }
}

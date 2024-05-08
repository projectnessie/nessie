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
import org.projectnessie.model.{
  Branch,
  ContentKey,
  MergeBehavior,
  MergeKeyBehavior,
  Tag
}

case class MergeBranchExec(
    output: Seq[Attribute],
    ref: Option[String],
    refTimestampOrHash: Option[String],
    toRefName: Option[String],
    dryRun: Boolean,
    defaultMergeBehavior: Option[String],
    keyMergeBehaviors: java.util.Map[String, String],
    currentCatalog: CatalogPlugin,
    catalog: Option[String]
) extends NessieExec(catalog = catalog, currentCatalog = currentCatalog)
    with LeafV2CommandExec {

  override protected def runInternal(
      bridge: CatalogBridge
  ): Seq[InternalRow] = {
    var from = bridge.api.getReference
      .refName(
        ref
          .map(unquoteRefName)
          .getOrElse(
            bridge.getCurrentRef.getName
          )
      )
      .get()
    if (refTimestampOrHash.isDefined) {
      if (from.isInstanceOf[Branch]) {
        from = Branch.of(from.getName, refTimestampOrHash.get)
      }
      if (from.isInstanceOf[Tag]) {
        from = Tag.of(from.getName, refTimestampOrHash.get)
      }
    }

    val merge = bridge.api
      .mergeRefIntoBranch()
      .branchName(
        toRefName
          .map(unquoteRefName)
          .getOrElse(bridge.api.getDefaultBranch.getName)
      )
      .hash(
        toRefName
          .map(unquoteRefName)
          .map(r => bridge.api.getReference.refName(r).get.getHash)
          .getOrElse(bridge.api.getDefaultBranch.getHash)
      )
      .fromRef(from)
      .dryRun(dryRun)

    defaultMergeBehavior.foreach(b =>
      merge.defaultMergeMode(MergeBehavior.valueOf(b))
    )
    keyMergeBehaviors.forEach((k, v) => {
      val key = ContentKey.fromPathString(k)
      merge.mergeKeyBehavior(MergeKeyBehavior.of(key, MergeBehavior.valueOf(v)))
    })

    merge.merge()

    val refObj = bridge.api.getReference.refName(
      toRefName
        .map(unquoteRefName)
        .getOrElse(bridge.api.getDefaultBranch.getName)
    )

    Seq(
      InternalRow(
        UTF8String.fromString(refObj.get.getName),
        UTF8String.fromString(refObj.get.getHash)
      )
    )
  }

  override def simpleString(maxFields: Int): String = {
    s"MergeBranchExec ${catalog.getOrElse(currentCatalog.name())} ${ref.map(unquoteRefName)} "
  }
}

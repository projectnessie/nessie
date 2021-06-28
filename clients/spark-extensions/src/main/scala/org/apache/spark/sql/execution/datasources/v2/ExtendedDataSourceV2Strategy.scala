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

import org.apache.spark.sql.{SparkSession, Strategy}
import org.apache.spark.sql.catalyst.plans.logical.{
  CreateReferenceCommand,
  DropReferenceCommand,
  ListReferenceCommand,
  LogicalPlan,
  MergeBranchCommand,
  ShowLogCommand,
  ShowReferenceCommand,
  UseReferenceCommand
}
import org.apache.spark.sql.execution.SparkPlan

case class ExtendedDataSourceV2Strategy(spark: SparkSession) extends Strategy {

  override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {

    case c @ CreateReferenceCommand(branch, isBranch, catalog, reference) =>
      CreateReferenceExec(
        c.output,
        branch,
        spark.sessionState.catalogManager.currentCatalog,
        isBranch,
        catalog,
        reference
      ) :: Nil

    case c @ DropReferenceCommand(branch, isBranch, catalog) =>
      DropReferenceExec(
        c.output,
        branch,
        spark.sessionState.catalogManager.currentCatalog,
        isBranch,
        catalog
      ) :: Nil

    case c @ UseReferenceCommand(branch, ts, catalog) =>
      UseReferenceExec(
        c.output,
        branch,
        spark.sessionState.catalogManager.currentCatalog,
        ts,
        catalog
      ) :: Nil

    case c @ ListReferenceCommand(catalog) =>
      ListReferenceExec(
        c.output,
        spark.sessionState.catalogManager.currentCatalog,
        catalog
      ) :: Nil

    case c @ ShowReferenceCommand(catalog) =>
      ShowReferenceExec(
        c.output,
        spark.sessionState.catalogManager.currentCatalog,
        catalog
      ) :: Nil

    case c @ MergeBranchCommand(branch, toRefName, catalog) =>
      MergeBranchExec(
        c.output,
        branch,
        spark.sessionState.catalogManager.currentCatalog,
        toRefName,
        catalog
      ) :: Nil

    case c @ ShowLogCommand(refName, catalog) =>
      ShowLogExec(
        c.output,
        refName,
        spark.sessionState.catalogManager.currentCatalog,
        catalog
      ) :: Nil

    case _ => Nil
  }

}

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
package org.apache.spark.sql.catalyst.parser.extensions

import org.projectnessie.shaded.org.antlr.v4.runtime.{ParserRuleContext, Token}
import org.projectnessie.shaded.org.antlr.v4.runtime.misc.Interval
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.parser.extensions.NessieParserUtils.withOrigin
import org.apache.spark.sql.catalyst.parser.extensions.NessieSqlExtensionsParser._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.trees.{CurrentOrigin, Origin}

class NessieSqlExtensionsAstBuilder(delegate: ParserInterface)
    extends NessieSqlExtensionsBaseVisitor[AnyRef] {

  override def visitNessieCreateRef(
      ctx: NessieCreateRefContext
  ): CreateReferenceCommand = withOrigin(ctx) {
    val isBranch = ctx.TAG == null
    val refName = ctx.reference.getText
    val failOnCreate = ctx.IF() == null && ctx.NOT() == null && ctx
      .EXISTS() == null
    val catalogName = asText(ctx.catalog)
    val createdFrom = asText(ctx.fromRef)
    CreateReferenceCommand(
      refName,
      isBranch,
      catalogName,
      createdFrom,
      failOnCreate
    )
  }

  override def visitNessieDropRef(
      ctx: NessieDropRefContext
  ): DropReferenceCommand = withOrigin(ctx) {
    val isBranch = ctx.TAG == null
    val refName = ctx.reference.getText
    val catalogName = asText(ctx.catalog)
    val failOnDrop = ctx.IF() == null && ctx.EXISTS() == null
    DropReferenceCommand(refName, isBranch, catalogName, failOnDrop)
  }

  override def visitNessieUseRef(
      ctx: NessieUseRefContext
  ): UseReferenceCommand =
    withOrigin(ctx) {
      val refName = ctx.reference.getText
      val timestampOrHash = asText(ctx.tsOrHash)
      val catalogName = asText(ctx.catalog)
      UseReferenceCommand(refName, timestampOrHash, catalogName)
    }

  override def visitNessieListRef(
      ctx: NessieListRefContext
  ): ListReferenceCommand = withOrigin(ctx) {
    val catalogName = asText(ctx.catalog)
    ListReferenceCommand(catalogName)
  }

  override def visitNessieShowRef(
      ctx: NessieShowRefContext
  ): ShowReferenceCommand = withOrigin(ctx) {
    val catalogName = asText(ctx.catalog)
    ShowReferenceCommand(catalogName)
  }

  override def visitNessieMergeRef(
      ctx: NessieMergeRefContext
  ): MergeBranchCommand = withOrigin(ctx) {
    val refName = asText(ctx.reference)
    val toRefName = asText(ctx.toRef)
    val catalogName = asText(ctx.catalog)
    MergeBranchCommand(refName, toRefName, catalogName)
  }

  override def visitNessieShowLog(
      ctx: NessieShowLogContext
  ): ShowLogCommand =
    withOrigin(ctx) {
      val refName = asText(ctx.reference)
      val timestampOrHash = asText(ctx.tsOrHash)
      val catalogName = asText(ctx.catalog)
      ShowLogCommand(refName, timestampOrHash, catalogName)
    }

  override def visitSingleStatement(ctx: SingleStatementContext): LogicalPlan =
    withOrigin(ctx) {
      visit(ctx.statement).asInstanceOf[LogicalPlan]
    }

  override def visitNessieAssignRef(
      ctx: NessieAssignRefContext
  ): AssignReferenceCommand =
    withOrigin(ctx) {
      val isBranch = ctx.TAG == null
      val refName = ctx.reference.getText
      val toRefName = asText(ctx.toRef)
      val toHash = asText(ctx.toHash)
      val catalogName = asText(ctx.catalog)
      AssignReferenceCommand(
        refName,
        isBranch,
        toRefName,
        toHash,
        catalogName
      )
    }

  private def asText(parameter: IdentifierContext): Option[String] = {
    Option(parameter).map(x => x.getText)
  }
}

/* Partially copied from Apache Spark's Parser to avoid dependency on Spark Internals */
object NessieParserUtils {

  private[sql] def withOrigin[T](ctx: ParserRuleContext)(f: => T): T = {
    val current = CurrentOrigin.get
    CurrentOrigin.set(position(ctx.getStart))
    try {
      f
    } finally {
      CurrentOrigin.set(current)
    }
  }

  private[sql] def position(token: Token): Origin = {
    val opt = Option(token)
    Origin(opt.map(_.getLine), opt.map(_.getCharPositionInLine))
  }

  /** Get the command which created the token. */
  private[sql] def command(ctx: ParserRuleContext): String = {
    val stream = ctx.getStart.getInputStream
    stream.getText(Interval.of(0, stream.size() - 1))
  }
}

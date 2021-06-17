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

import org.antlr.v4.runtime._
import org.antlr.v4.runtime.misc.Interval
import org.antlr.v4.runtime.tree.{ParseTree, TerminalNode}
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.parser.extensions.NessieParserUtils.withOrigin
import org.apache.spark.sql.catalyst.parser.extensions.NessieSqlExtensionsParser._
import org.apache.spark.sql.catalyst.plans.logical.{
  CreateReferenceField,
  DropReferenceField,
  ListReferenceField,
  LogicalPlan,
  MergeBranchField,
  ShowLogField,
  ShowReferenceField,
  UseReferenceField
}
import org.apache.spark.sql.catalyst.trees.{CurrentOrigin, Origin}

import scala.collection.JavaConverters._

class NessieSqlExtensionsAstBuilder(delegate: ParserInterface)
    extends NessieSqlExtensionsBaseVisitor[AnyRef] {

  override def visitNessieCreateRef(
      ctx: NessieCreateRefContext
  ): CreateReferenceField = withOrigin(ctx) {
    val isBranch = ctx.TAG == null
    val refName = ctx.identifier(0).getText
    val catalogName = asText(ctx.catalog)
    val createdFrom = asText(ctx.reference)
    CreateReferenceField(refName, isBranch, catalogName, createdFrom)
  }

  override def visitNessieDropRef(
      ctx: NessieDropRefContext
  ): DropReferenceField = withOrigin(ctx) {
    val isBranch = ctx.TAG == null
    val refName = ctx.identifier(0).getText
    val catalogName = asText(ctx.catalog)
    DropReferenceField(refName, isBranch, catalogName)
  }

  override def visitNessieUseRef(ctx: NessieUseRefContext): UseReferenceField =
    withOrigin(ctx) {
      val refName = ctx.identifier(0).getText
      val timestamp = asText(ctx.ts)
      val catalogName = asText(ctx.catalog)
      UseReferenceField(refName, timestamp, catalogName)
    }

  override def visitNessieListRef(
      ctx: NessieListRefContext
  ): ListReferenceField = withOrigin(ctx) {
    val catalogName = asText(ctx.catalog)
    ListReferenceField(catalogName)
  }

  override def visitNessieShowRef(
      ctx: NessieShowRefContext
  ): ShowReferenceField = withOrigin(ctx) {
    val catalogName = asText(ctx.catalog)
    ShowReferenceField(catalogName)
  }

  override def visitNessieMergeRef(
      ctx: NessieMergeRefContext
  ): MergeBranchField = withOrigin(ctx) {
    val refName = asText(ctx.identifier(0))
    val toRefName = asText(ctx.toRef)
    val catalogName = asText(ctx.catalog)
    MergeBranchField(refName, toRefName, catalogName)
  }

  override def visitNessieShowLog(ctx: NessieShowLogContext): ShowLogField =
    withOrigin(ctx) {
      val refName = asText(ctx.identifier(0))
      val catalogName = asText(ctx.catalog)
      ShowLogField(refName, catalogName)
    }

  override def visitSingleStatement(ctx: SingleStatementContext): LogicalPlan =
    withOrigin(ctx) {
      visit(ctx.statement).asInstanceOf[LogicalPlan]
    }

  private def reconstructSqlString(ctx: ParserRuleContext): String = {
    ctx.children.asScala
      .map {
        case c: ParserRuleContext => reconstructSqlString(c)
        case t: TerminalNode      => t.getText
      }
      .mkString(" ")
  }

  private def typedVisit[T](ctx: ParseTree): T = {
    ctx.accept(this).asInstanceOf[T]
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

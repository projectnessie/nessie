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
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal}
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.parser.extensions.NessieParserUtils.withOrigin
import org.apache.spark.sql.catalyst.parser.extensions.NessieSqlExtensionsParser._
import org.apache.spark.sql.catalyst.plans.logical.{
  CallArgument,
  CreateReferenceField,
  DropReferenceField,
  ListReferenceField,
  LogicalPlan,
  MergeBranchField,
  NamedArgument,
  PositionalArgument,
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
    val catalogName = Option(ctx.catalog).map(x => x.getText)
    val createdFrom = Option(ctx.reference).map(x => x.getText)
    CreateReferenceField(refName, isBranch, catalogName, createdFrom)
  }

  override def visitNessieDropRef(
      ctx: NessieDropRefContext
  ): DropReferenceField = withOrigin(ctx) {
    val isBranch = ctx.TAG == null
    val refName = ctx.identifier(0).getText
    val catalogName = Option(ctx.catalog).map(x => x.getText)
    DropReferenceField(refName, isBranch, catalogName)
  }

  override def visitNessieUseRef(ctx: NessieUseRefContext): UseReferenceField =
    withOrigin(ctx) {
      val refName = ctx.identifier(0).getText
      val timestamp = Option(ctx.ts).map(x => x.getText)
      val catalogName = Option(ctx.catalog).map(x => x.getText)
      UseReferenceField(refName, timestamp, catalogName)
    }

  override def visitNessieListRef(
      ctx: NessieListRefContext
  ): ListReferenceField = withOrigin(ctx) {
    val catalogName = Option(ctx.catalog).map(x => x.getText)
    ListReferenceField(catalogName)
  }

  override def visitNessieShowRef(
      ctx: NessieShowRefContext
  ): ShowReferenceField = withOrigin(ctx) {
    val catalogName = Option(ctx.catalog).map(x => x.getText)
    ShowReferenceField(catalogName)
  }

  override def visitNessieMergeRef(
      ctx: NessieMergeRefContext
  ): MergeBranchField = withOrigin(ctx) {
    val refName = Option(ctx.identifier(0)).map(x => x.getText)
    val toRefName = Option(ctx.toRef).map(x => x.getText)
    val catalogName = Option(ctx.catalog).map(x => x.getText)
    MergeBranchField(refName, toRefName, catalogName)
  }

  override def visitNessieShowLog(ctx: NessieShowLogContext): ShowLogField =
    withOrigin(ctx) {
      val refName = Option(ctx.identifier(0)).map(x => x.getText)
      val catalogName = Option(ctx.catalog).map(x => x.getText)
      ShowLogField(refName, catalogName)
    }

  /**
    * Return a multi-part identifier as Seq[String].
    */
  override def visitMultipartIdentifier(
      ctx: MultipartIdentifierContext
  ): Seq[String] = withOrigin(ctx) {
    ctx.parts.asScala.map(_.getText)
  }

  /**
    * Create a positional argument in a stored procedure call.
    */
  override def visitPositionalArgument(
      ctx: PositionalArgumentContext
  ): CallArgument = withOrigin(ctx) {
    val expr = typedVisit[Expression](ctx.expression)
    PositionalArgument(expr)
  }

  /**
    * Create a named argument in a stored procedure call.
    */
  override def visitNamedArgument(ctx: NamedArgumentContext): CallArgument =
    withOrigin(ctx) {
      val name = ctx.identifier.getText
      val expr = typedVisit[Expression](ctx.expression)
      NamedArgument(name, expr)
    }

  override def visitSingleStatement(ctx: SingleStatementContext): LogicalPlan =
    withOrigin(ctx) {
      visit(ctx.statement).asInstanceOf[LogicalPlan]
    }

  def visitConstant(ctx: ConstantContext): Literal = {
    delegate.parseExpression(ctx.getText).asInstanceOf[Literal]
  }

  override def visitExpression(ctx: ExpressionContext): Expression = {
    // reconstruct the SQL string and parse it using the main Spark parser
    // while we can avoid the logic to build Spark expressions, we still have to parse them
    // we cannot call ctx.getText directly since it will not render spaces correctly
    // that's why we need to recurse down the tree in reconstructSqlString
    val sqlString = reconstructSqlString(ctx)
    delegate.parseExpression(sqlString)
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

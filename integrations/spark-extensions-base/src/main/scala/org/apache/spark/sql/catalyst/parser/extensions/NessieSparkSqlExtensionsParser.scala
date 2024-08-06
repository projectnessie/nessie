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

import org.projectnessie.nessie.cli.grammar.{
  NessieCliLexer,
  NessieCliParser,
  ParseException
}
import org.projectnessie.nessie.cli.grammar.Token.TokenType
import org.apache.spark.sql.catalyst.{FunctionIdentifier, TableIdentifier}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.plans.logical.{
  AssignReferenceCommand,
  CreateReferenceCommand,
  DropReferenceCommand,
  ListReferenceCommand,
  LogicalPlan,
  MergeBranchCommand,
  ShowLogCommand,
  ShowReferenceCommand,
  UseReferenceCommand
}
import org.apache.spark.sql.internal.{SQLConf, VariableSubstitution}
import org.apache.spark.sql.types.{DataType, StructType}
import org.projectnessie.nessie.cli.cmdspec.{
  AssignReferenceCommandSpec,
  CommandType,
  CreateReferenceCommandSpec,
  DropReferenceCommandSpec,
  ListReferencesCommandSpec,
  MergeBranchCommandSpec,
  ShowLogCommandSpec,
  ShowReferenceCommandSpec,
  UseReferenceCommandSpec
}
import org.projectnessie.nessie.cli.grammar.ast.SingleStatement

import scala.util.Try

class NessieSparkSqlExtensionsParser(delegate: ParserInterface)
    extends ParserInterface {

  import NessieSparkSqlExtensionsParser._

  private lazy val substitutor = {
    Try(substitutorCtor.newInstance(SQLConf.get))
      .getOrElse(substitutorCtor.newInstance())
  }

  /** Parse a string to a DataType.
    */
  override def parseDataType(sqlText: String): DataType = {
    delegate.parseDataType(sqlText)
  }

  /** Parse a string to a raw DataType without CHAR/VARCHAR replacement.
    */
  def parseRawDataType(sqlText: String): DataType =
    throw new UnsupportedOperationException()

  override def parseQuery(sqlText: String): LogicalPlan = {
    delegate.parseQuery(sqlText)
  }

  /** Parse a string to an Expression.
    */
  override def parseExpression(sqlText: String): Expression = {
    delegate.parseExpression(sqlText)
  }

  /** Parse a string to a TableIdentifier.
    */
  override def parseTableIdentifier(sqlText: String): TableIdentifier = {
    delegate.parseTableIdentifier(sqlText)
  }

  /** Parse a string to a FunctionIdentifier.
    */
  override def parseFunctionIdentifier(sqlText: String): FunctionIdentifier = {
    delegate.parseFunctionIdentifier(sqlText)
  }

  /** Parse a string to a multi-part identifier.
    */
  override def parseMultipartIdentifier(sqlText: String): Seq[String] = {
    delegate.parseMultipartIdentifier(sqlText)
  }

  /** Creates StructType for a given SQL string, which is a comma separated list
    * of field definitions which will preserve the correct Hive metadata.
    */
  override def parseTableSchema(sqlText: String): StructType = {
    delegate.parseTableSchema(sqlText)
  }

  /** Parse a string to a LogicalPlan.
    */
  override def parsePlan(sqlText: String): LogicalPlan = {
    val sqlTextAfterSubstitution = substitutor.substitute(sqlText)
    val lexer = new NessieCliLexer(sqlTextAfterSubstitution)
    val parser = new NessieCliParser(lexer)
    parser.deactivateTokenType(TokenType.CONNECT)
    parser.deactivateTokenType(TokenType.EXIT)
    parser.deactivateTokenType(TokenType.HELP)
    parser.activateTokenType(TokenType.IN)
    try {
      parser.SingleStatement()

      val root = parser.rootNode
      val singleStatement = root.asInstanceOf[SingleStatement]
      val commandSpec = singleStatement.getCommandSpec

      commandSpec.commandType() match {
        // case CommandType.CONNECT => // disabled above
        // case CommandType.EXIT => // disabled above
        // case CommandType.HELP => // disabled above
        case CommandType.USE_REFERENCE =>
          val spec = commandSpec.asInstanceOf[UseReferenceCommandSpec]
          return UseReferenceCommand(
            spec.getRef,
            Option(spec.getRefTimestampOrHash),
            Option(spec.getInCatalog)
          )
        case CommandType.CREATE_REFERENCE =>
          val spec = commandSpec.asInstanceOf[CreateReferenceCommandSpec]
          return CreateReferenceCommand(
            spec.getRef,
            Option(spec.getRefTimestampOrHash),
            spec.getRefType.equals("BRANCH"),
            Option(spec.getInCatalog),
            Option(spec.getFromRef),
            !spec.isConditional
          )
        case CommandType.DROP_REFERENCE =>
          val spec = commandSpec.asInstanceOf[DropReferenceCommandSpec]
          return DropReferenceCommand(
            spec.getRef,
            spec.getRefType.equals("BRANCH"),
            Option(spec.getInCatalog),
            !spec.isConditional
          )
        case CommandType.ASSIGN_REFERENCE =>
          val spec = commandSpec.asInstanceOf[AssignReferenceCommandSpec]
          return AssignReferenceCommand(
            spec.getRef,
            spec.getRefType.equals("BRANCH"),
            Option(spec.getTo),
            Option(spec.getRefTimestampOrHash),
            Option(spec.getInCatalog)
          )
        case CommandType.LIST_REFERENCES =>
          val spec = commandSpec.asInstanceOf[ListReferencesCommandSpec]
          return ListReferenceCommand(
            Option(spec.getFilter),
            Option(spec.getStartsWith),
            Option(spec.getContains),
            Option(spec.getInCatalog)
          )
        case CommandType.MERGE_BRANCH =>
          val spec = commandSpec.asInstanceOf[MergeBranchCommandSpec]
          return MergeBranchCommand(
            Option(spec.getRef),
            Option(spec.getRefTimestampOrHash),
            Option(spec.getInto),
            spec.isDryRun,
            Option(spec.getDefaultMergeBehavior),
            spec.getKeyMergeBehaviors,
            Option(spec.getInCatalog)
          )
        case CommandType.SHOW_LOG =>
          val spec = commandSpec.asInstanceOf[ShowLogCommandSpec]
          return ShowLogCommand(
            Option(spec.getRef),
            Option(spec.getRefTimestampOrHash),
            Option(spec.getLimit).flatMap(l => if (l == 0) None else Option(l)),
            Option(spec.getInCatalog)
          )
        case CommandType.SHOW_REFERENCE =>
          val spec = commandSpec.asInstanceOf[ShowReferenceCommandSpec]
          return ShowReferenceCommand(
            Option(spec.getRef),
            Option(spec.getRefTimestampOrHash),
            Option(spec.getInCatalog)
          )
        // case CommandType.SHOW_CONTENT   => ???
        // case CommandType.REVERT_CONTENT => ???
        // case CommandType.CREATE_NAMESPACE => ???
        // case CommandType.DROP_CONTENT => ???
        // case CommandType.ALTER_NAMESPACE => ???
        // case CommandType.LIST_CONTENTS   => ???
        case _ => delegate.parsePlan(sqlText)
      }

    } catch {
      case _: ParseException =>
      // TODO check whether it's a Nessie statement
      //        delegate.parsePlan(sqlText)
      //        val token = e.getToken
      //        val start =
      //          if (token != null)
      //            Origin(
      //              Option(token.getBeginLine),
      //              Option(token.getBeginColumn),
      //              Option(token.getBeginOffset)
      //            )
      //          else Origin()
      //        val stop =
      //          if (token != null)
      //            Origin(
      //              Option(token.getEndLine),
      //              Option(token.getEndColumn),
      //              Option(token.getEndOffset)
      //            )
      //          else Origin()
      //        throw new NessieParseException(
      //          Option(sqlTextAfterSubstitution),
      //          e.getMessage,
      //          start,
      //          stop
      //        )
    }

    delegate.parsePlan(sqlText)
  }
}

object NessieSparkSqlExtensionsParser {
  private val substitutorCtor = {
    Try(classOf[VariableSubstitution].getConstructor(classOf[SQLConf]))
      .getOrElse(classOf[VariableSubstitution].getConstructor())
  }
}

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

import org.projectnessie.shaded.org.antlr.v4.runtime.{
  BaseErrorListener,
  CharStream,
  CharStreams,
  CodePointCharStream,
  CommonToken,
  CommonTokenStream,
  IntStream,
  ParserRuleContext,
  RecognitionException,
  Recognizer
}
import org.projectnessie.shaded.org.antlr.v4.runtime.atn.PredictionMode
import org.projectnessie.shaded.org.antlr.v4.runtime.misc.{
  Interval,
  ParseCancellationException
}
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.{FunctionIdentifier, TableIdentifier}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.trees.Origin
import org.apache.spark.sql.internal.{SQLConf, VariableSubstitution}
import org.apache.spark.sql.types.{DataType, StructType}

import java.util.Locale
import scala.util.Try

class NessieSparkSqlExtensionsParser(delegate: ParserInterface)
    extends ParserInterface {

  import NessieSparkSqlExtensionsParser._

  private lazy val substitutor = {
    Try(substitutorCtor.newInstance(SQLConf.get))
      .getOrElse(substitutorCtor.newInstance())
  }

  private lazy val astBuilder = new NessieSqlExtensionsAstBuilder(delegate)

  /**
    * Parse a string to a DataType.
    */
  override def parseDataType(sqlText: String): DataType = {
    delegate.parseDataType(sqlText)
  }

  /**
    * Parse a string to a raw DataType without CHAR/VARCHAR replacement.
    */
  def parseRawDataType(sqlText: String): DataType =
    throw new UnsupportedOperationException()

  /**
    * Parse a string to an Expression.
    */
  override def parseExpression(sqlText: String): Expression = {
    delegate.parseExpression(sqlText)
  }

  /**
    * Parse a string to a TableIdentifier.
    */
  override def parseTableIdentifier(sqlText: String): TableIdentifier = {
    delegate.parseTableIdentifier(sqlText)
  }

  /**
    * Parse a string to a FunctionIdentifier.
    */
  override def parseFunctionIdentifier(sqlText: String): FunctionIdentifier = {
    delegate.parseFunctionIdentifier(sqlText)
  }

  /**
    * Parse a string to a multi-part identifier.
    */
  override def parseMultipartIdentifier(sqlText: String): Seq[String] = {
    delegate.parseMultipartIdentifier(sqlText)
  }

  /**
    * Creates StructType for a given SQL string, which is a comma separated list of field
    * definitions which will preserve the correct Hive metadata.
    */
  override def parseTableSchema(sqlText: String): StructType = {
    delegate.parseTableSchema(sqlText)
  }

  /**
    * Parse a string to a LogicalPlan.
    */
  override def parsePlan(sqlText: String): LogicalPlan = {
    val sqlTextAfterSubstitution = substitutor.substitute(sqlText)
    if (isNessieCommand(sqlTextAfterSubstitution)) {
      parse(sqlTextAfterSubstitution) { parser =>
        astBuilder.visit(parser.singleStatement())
      }.asInstanceOf[LogicalPlan]
    } else {
      delegate.parsePlan(sqlText)
    }
  }

  private def isNessieCommand(sqlText: String): Boolean = {
    val normalized = sqlText.toLowerCase(Locale.ROOT).trim()
    normalized.startsWith("create branch") || normalized.startsWith(
      "create tag"
    ) ||
    normalized.startsWith("drop branch") || normalized.startsWith("drop tag") ||
    normalized.startsWith("use reference") || normalized.startsWith(
      "list reference"
    ) ||
    normalized.startsWith("show reference") || normalized.startsWith("show log") ||
    normalized.startsWith("merge branch") || normalized.startsWith(
      "assign branch"
    ) ||
    normalized.startsWith("assign tag")
  }

  protected def parse[T](
      command: String
  )(toResult: NessieSqlExtensionsParser => T): T = {
    val lexer = new NessieSqlExtensionsLexer(
      new NessieUpperCaseCharStream(CharStreams.fromString(command))
    )
    lexer.removeErrorListeners()
    lexer.addErrorListener(NessieParseErrorListener)

    val tokenStream = new CommonTokenStream(lexer)
    val parser = new NessieSqlExtensionsParser(tokenStream)
    parser.removeErrorListeners()
    parser.addErrorListener(NessieParseErrorListener)

    try {
      try {
        // first, try parsing with potentially faster SLL mode
        parser.getInterpreter.setPredictionMode(PredictionMode.SLL)
        toResult(parser)
      } catch {
        case _: ParseCancellationException =>
          // if we fail, parse with LL mode
          tokenStream.seek(0) // rewind input stream
          parser.reset()

          // Try Again.
          parser.getInterpreter.setPredictionMode(PredictionMode.LL)
          toResult(parser)
      }
    } catch {
      case e: NessieParseException if e.command.isDefined =>
        throw e
      case e: NessieParseException =>
        throw e.withCommand(command)
      case e: AnalysisException =>
        val position = Origin(e.line, e.startPosition)
        throw new NessieParseException(
          Option(command),
          e.message,
          position,
          position
        )
    }
  }
}

object NessieSparkSqlExtensionsParser {
  private val substitutorCtor = {
    Try(classOf[VariableSubstitution].getConstructor(classOf[SQLConf]))
      .getOrElse(classOf[VariableSubstitution].getConstructor())
  }
}

/* Copied from Apache Spark's to avoid dependency on Spark Internals */
class NessieUpperCaseCharStream(wrapped: CodePointCharStream)
    extends CharStream {
  override def consume(): Unit = wrapped.consume
  override def getSourceName(): String = wrapped.getSourceName
  override def index(): Int = wrapped.index
  override def mark(): Int = wrapped.mark
  override def release(marker: Int): Unit = wrapped.release(marker)
  override def seek(where: Int): Unit = wrapped.seek(where)
  override def size(): Int = wrapped.size

  override def getText(interval: Interval): String = {
    // ANTLR 4.7's CodePointCharStream implementations have bugs when
    // getText() is called with an empty stream, or intervals where
    // the start > end. See
    // https://github.com/antlr/antlr4/commit/ac9f7530 for one fix
    // that is not yet in a released ANTLR artifact.
    if (size() > 0 && (interval.b - interval.a >= 0)) {
      wrapped.getText(interval)
    } else {
      ""
    }
  }

  // scalastyle:off
  override def LA(i: Int): Int = {
    val la = wrapped.LA(i)
    if (la == 0 || la == IntStream.EOF) la
    else Character.toUpperCase(la)
  }
  // scalastyle:on
}

/* Partially copied from Apache Spark's Parser to avoid dependency on Spark Internals */
case object NessieParseErrorListener extends BaseErrorListener {
  override def syntaxError(
      recognizer: Recognizer[_, _],
      offendingSymbol: scala.Any,
      line: Int,
      charPositionInLine: Int,
      msg: String,
      e: RecognitionException
  ): Unit = {
    val (start, stop) = offendingSymbol match {
      case token: CommonToken =>
        val start = Origin(Some(line), Some(token.getCharPositionInLine))
        val length = token.getStopIndex - token.getStartIndex + 1
        val stop =
          Origin(Some(line), Some(token.getCharPositionInLine + length))
        (start, stop)
      case _ =>
        val start = Origin(Some(line), Some(charPositionInLine))
        (start, start)
    }
    throw new NessieParseException(None, msg, start, stop)
  }
}

/**
  * Copied from Apache Spark
  * A [[NessieParseException]] is an `AnalysisException` that is thrown during the parse process.
  * It contains fields and an extended error message that make reporting and diagnosing errors easier.
  */
class NessieParseException(
    val command: Option[String],
    message: String,
    val start: Origin,
    val stop: Origin
) extends AnalysisException(message, start.line, start.startPosition) {

  def this(message: String, ctx: ParserRuleContext) = {
    this(
      Option(NessieParserUtils.command(ctx)),
      message,
      NessieParserUtils.position(ctx.getStart),
      NessieParserUtils.position(ctx.getStop)
    )
  }

  override def getMessage: String = {
    val builder = new StringBuilder
    builder ++= "\n" ++= message
    start match {
      case Origin(Some(l), Some(p)) =>
        builder ++= s"(line $l, pos $p)\n"
        command.foreach { cmd =>
          val (above, below) = cmd.split("\n").splitAt(l)
          builder ++= "\n== SQL ==\n"
          above.foreach(builder ++= _ += '\n')
          builder ++= (0 until p).map(_ => "-").mkString("") ++= "^^^\n"
          below.foreach(builder ++= _ += '\n')
        }
      case _ =>
        command.foreach { cmd =>
          builder ++= "\n== SQL ==\n" ++= cmd
        }
    }
    builder.toString
  }

  def withCommand(cmd: String): NessieParseException = {
    new NessieParseException(Option(cmd), message, start, stop)
  }
}

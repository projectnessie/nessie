/*
 * Copyright (C) 2024 Dremio
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
package org.projectnessie.nessie.cli.syntax;

import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.projectnessie.nessie.cli.syntax.SyntaxTool.leadingTokens;

import java.nio.file.Paths;
import java.util.Map;
import java.util.stream.Stream;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.congocc.core.BNFProduction;
import org.congocc.core.Grammar;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

@ExtendWith(SoftAssertionsExtension.class)
public class TestSyntax {
  @InjectSoftAssertions protected SoftAssertions soft;

  @ParameterizedTest
  @MethodSource
  public void syntaxTest(String production, String expected, String plain) throws Exception {
    Syntax syntax =
        new Syntax(
            Paths.get("src/main/congocc/nessie/nessie-cli.ccc"), Map.of("omitSparkSql", "true"));
    Grammar grammar = syntax.getGrammar();
    BNFProduction prod = grammar.getProductionByName(production);

    StringBuilder sb = new StringBuilder();
    syntax.print(
        prod,
        new SyntaxPrinter() {
          @Override
          public void newline(String indent) {
            sb.append('\n').append(indent);
          }

          @Override
          public void space() {
            sb.append(' ');
          }

          @Override
          public void write(Type type, String str) {
            // Little weird syntax, but good enough for testing purposes.
            sb.append("{").append(type.name()).append(':').append(str).append('}');
          }
        });

    soft.assertThat(sb.toString()).isEqualTo(expected);

    String asPlain = SyntaxTool.syntaxAsPlain(leadingTokens(production), syntax, prod);
    soft.assertThat(asPlain).isEqualTo(plain);
  }

  static Stream<Arguments> syntaxTest() {
    return Stream.of(
        arguments(
            "RevertContentStatement",
            """
            {TERMINAL:REVERT} {TERMINAL:CONTENT}\s
                {PRE:[} {TERMINAL:DRY} {POST:]}\s
                {TERMINAL:OF} {NON_TERMINAL:ContentKey}\s
                {PRE:{} {TERMINAL:AND} {NON_TERMINAL:ContentKey} {POST:}}\s
                {PRE:[} {TERMINAL:ON} {PRE:[} {NON_TERMINAL:ReferenceType} {POST:]} {NON_TERMINAL:ExistingReference} {POST:]}\s
                {TERMINAL:TO} {TERMINAL:STATE}\s
                {PRE:(} {TERMINAL:ON} {PRE:[} {NON_TERMINAL:ReferenceType} {POST:]} {NON_TERMINAL:ExistingReference} {PRE:[} {TERMINAL:AT} {PRE:[} {TERMINAL:TIMESTAMP} {SEP:|} {TERMINAL:COMMIT} {POST:]} {NON_TERMINAL:TimestampOrCommit} {POST:]}
                {SEP:|} {TERMINAL:AT} {PRE:[} {TERMINAL:TIMESTAMP} {SEP:|} {TERMINAL:COMMIT} {POST:]} {NON_TERMINAL:TimestampOrCommit}
                {POST:)}\s
                {PRE:[} {TERMINAL:ALLOW} {TERMINAL:DELETES} {POST:]}""",
            """
            REVERT CONTENT\s
                [ DRY ]\s
                OF ContentKey\s
                { AND ContentKey }\s
                [ ON [ ReferenceType ] ExistingReference ]\s
                TO STATE\s
                ( ON [ ReferenceType ] ExistingReference [ AT [ TIMESTAMP | COMMIT ] TimestampOrCommit ]
                | AT [ TIMESTAMP | COMMIT ] TimestampOrCommit
                )\s
                [ ALLOW DELETES ]"""),
        arguments(
            "ShowReferenceStatement",
            """
            {TERMINAL:REFERENCE}\s
                {PRE:[} {NON_TERMINAL:ExistingReference} {POST:]}\s
                {PRE:[} {TERMINAL:AT} {PRE:[} {TERMINAL:TIMESTAMP} {SEP:|} {TERMINAL:COMMIT} {POST:]} {NON_TERMINAL:TimestampOrCommit} {POST:]}""",
            """
            SHOW REFERENCE\s
                [ ExistingReference ]\s
                [ AT [ TIMESTAMP | COMMIT ] TimestampOrCommit ]"""),
        arguments(
            "MergeBehaviorKind",
            "{TERMINAL:NORMAL} {SEP:|} {TERMINAL:FORCE} {SEP:|} {TERMINAL:DROP}",
            "NORMAL | FORCE | DROP"),
        arguments("ReferenceType", "{TERMINAL:BRANCH} {SEP:|} {TERMINAL:TAG}", "BRANCH | TAG"),
        arguments(
            "HelpStatement",
            """
            {TERMINAL:HELP}\s
                {PRE:[} {TERMINAL:USE}
                {SEP:|} {TERMINAL:CONNECT}
                {SEP:|} {TERMINAL:CREATE} {PRE:[} {PRE:(} {TERMINAL:BRANCH} {SEP:|} {TERMINAL:TAG} {SEP:|} {TERMINAL:NAMESPACE} {POST:)} {POST:]}
                {SEP:|} {TERMINAL:ALTER} {PRE:[} {TERMINAL:NAMESPACE} {POST:]}
                {SEP:|} {TERMINAL:DROP} {PRE:[} {TERMINAL:BRANCH} {SEP:|} {TERMINAL:TAG} {SEP:|} {TERMINAL:NAMESPACE} {SEP:|} {TERMINAL:TABLE} {SEP:|} {TERMINAL:VIEW} {POST:]}
                {SEP:|} {TERMINAL:LIST} {PRE:[} {TERMINAL:CONTENTS} {SEP:|} {TERMINAL:REFERENCES} {POST:]}
                {SEP:|} {TERMINAL:SHOW} {PRE:[} {TERMINAL:LOG} {SEP:|} {TERMINAL:TABLE} {SEP:|} {TERMINAL:VIEW} {SEP:|} {TERMINAL:NAMESPACE} {SEP:|} {TERMINAL:REFERENCE} {POST:]}
                {SEP:|} {TERMINAL:ASSIGN} {PRE:[} {TERMINAL:BRANCH} {SEP:|} {TERMINAL:TAG} {POST:]}
                {SEP:|} {TERMINAL:MERGE}
                {SEP:|} {TERMINAL:REVERT}
                {SEP:|} {TERMINAL:HELP}
                {SEP:|} {TERMINAL:EXIT}
                {SEP:|} {TERMINAL:LICENSE}
                {POST:]}""",
            """
            HELP\s
                [ USE
                | CONNECT
                | CREATE [ ( BRANCH | TAG | NAMESPACE ) ]
                | ALTER [ NAMESPACE ]
                | DROP [ BRANCH | TAG | NAMESPACE | TABLE | VIEW ]
                | LIST [ CONTENTS | REFERENCES ]
                | SHOW [ LOG | TABLE | VIEW | NAMESPACE | REFERENCE ]
                | ASSIGN [ BRANCH | TAG ]
                | MERGE
                | REVERT
                | HELP
                | EXIT
                | LICENSE
                ]"""),
        arguments(
            "CreateNamespaceStatement",
            """
            {TERMINAL:NAMESPACE} {NON_TERMINAL:ContentKey}\s
                {PRE:[} {TERMINAL:ON} {PRE:[} {NON_TERMINAL:ReferenceType} {POST:]} {NON_TERMINAL:ExistingReference} {POST:]}\s
                {PRE:[} {TERMINAL:SET} {NON_TERMINAL:ParamKey} {TERMINAL:=} {NON_TERMINAL:Value} {PRE:{} {TERMINAL:AND} {NON_TERMINAL:ParamKey} {TERMINAL:=} {NON_TERMINAL:Value} {POST:}} {POST:]}""",
            """
            CREATE NAMESPACE ContentKey\s
                [ ON [ ReferenceType ] ExistingReference ]\s
                [ SET ParamKey = Value { AND ParamKey = Value } ]"""),
        arguments(
            "CreateReferenceStatement",
            """
            {NON_TERMINAL:ReferenceType}\s
                {PRE:[} {TERMINAL:IF} {TERMINAL:NOT} {TERMINAL:EXISTS} {POST:]}\s
                {NON_TERMINAL:ReferenceName}\s
                {PRE:[} {TERMINAL:FROM} {NON_TERMINAL:ExistingReference} {POST:]}\s
                {PRE:[} {TERMINAL:AT} {PRE:[} {TERMINAL:TIMESTAMP} {SEP:|} {TERMINAL:COMMIT} {POST:]} {NON_TERMINAL:TimestampOrCommit} {POST:]}""",
            """
            CREATE ReferenceType\s
                [ IF NOT EXISTS ]\s
                ReferenceName\s
                [ FROM ExistingReference ]\s
                [ AT [ TIMESTAMP | COMMIT ] TimestampOrCommit ]"""),
        arguments(
            "MergeBranchStatement",
            """
            {TERMINAL:MERGE}\s
                {PRE:[} {TERMINAL:DRY} {POST:]}\s
                {PRE:[} {NON_TERMINAL:ReferenceType} {POST:]}\s
                {NON_TERMINAL:ExistingReference}\s
                {PRE:[} {TERMINAL:AT} {PRE:[} {TERMINAL:TIMESTAMP} {SEP:|} {TERMINAL:COMMIT} {POST:]} {NON_TERMINAL:TimestampOrCommit} {POST:]}\s
                {PRE:[} {TERMINAL:INTO} {NON_TERMINAL:ExistingReference} {POST:]}\s
                {PRE:[} {TERMINAL:BEHAVIOR} {NON_TERMINAL:MergeBehaviorKind} {POST:]}\s
                {PRE:[} {TERMINAL:BEHAVIORS} {NON_TERMINAL:ContentKey} {TERMINAL:=} {NON_TERMINAL:MergeBehaviorKind} {PRE:{} {TERMINAL:AND} {NON_TERMINAL:ContentKey} {TERMINAL:=} {NON_TERMINAL:MergeBehaviorKind} {POST:}} {POST:]}""",
            """
            MERGE\s
                [ DRY ]\s
                [ ReferenceType ]\s
                ExistingReference\s
                [ AT [ TIMESTAMP | COMMIT ] TimestampOrCommit ]\s
                [ INTO ExistingReference ]\s
                [ BEHAVIOR MergeBehaviorKind ]\s
                [ BEHAVIORS ContentKey = MergeBehaviorKind { AND ContentKey = MergeBehaviorKind } ]"""));
  }
}

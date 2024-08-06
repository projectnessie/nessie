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
            "{TERMINAL:REVERT} {TERMINAL:CONTENT} \n"
                + "    {PRE:[} {TERMINAL:DRY} {POST:]} \n"
                + "    {TERMINAL:OF} {NON_TERMINAL:ContentKey} \n"
                + "    {PRE:{} {TERMINAL:AND} {NON_TERMINAL:ContentKey} {POST:}} \n"
                + "    {PRE:[} {TERMINAL:ON} {PRE:[} {NON_TERMINAL:ReferenceType} {POST:]} {NON_TERMINAL:ExistingReference} {POST:]} \n"
                + "    {TERMINAL:TO} {TERMINAL:STATE} \n"
                + "    {PRE:(} {TERMINAL:ON} {PRE:[} {NON_TERMINAL:ReferenceType} {POST:]} {NON_TERMINAL:ExistingReference} {PRE:[} {TERMINAL:AT} {PRE:[} {TERMINAL:TIMESTAMP} {SEP:|} {TERMINAL:COMMIT} {POST:]} {NON_TERMINAL:TimestampOrCommit} {POST:]}\n"
                + "    {SEP:|} {TERMINAL:AT} {PRE:[} {TERMINAL:TIMESTAMP} {SEP:|} {TERMINAL:COMMIT} {POST:]} {NON_TERMINAL:TimestampOrCommit}\n"
                + "    {POST:)} \n"
                + "    {PRE:[} {TERMINAL:ALLOW} {TERMINAL:DELETES} {POST:]}",
            "REVERT CONTENT \n"
                + "    [ DRY ] \n"
                + "    OF ContentKey \n"
                + "    { AND ContentKey } \n"
                + "    [ ON [ ReferenceType ] ExistingReference ] \n"
                + "    TO STATE \n"
                + "    ( ON [ ReferenceType ] ExistingReference [ AT [ TIMESTAMP | COMMIT ] TimestampOrCommit ]\n"
                + "    | AT [ TIMESTAMP | COMMIT ] TimestampOrCommit\n"
                + "    ) \n"
                + "    [ ALLOW DELETES ]"),
        arguments(
            "ShowReferenceStatement",
            "{TERMINAL:REFERENCE} \n"
                + "    {PRE:[} {NON_TERMINAL:ExistingReference} {POST:]} \n"
                + "    {PRE:[} {TERMINAL:AT} {PRE:[} {TERMINAL:TIMESTAMP} {SEP:|} {TERMINAL:COMMIT} {POST:]} {NON_TERMINAL:TimestampOrCommit} {POST:]}",
            "SHOW REFERENCE \n"
                + "    [ ExistingReference ] \n"
                + "    [ AT [ TIMESTAMP | COMMIT ] TimestampOrCommit ]"),
        arguments(
            "MergeBehaviorKind",
            "{TERMINAL:NORMAL} {SEP:|} {TERMINAL:FORCE} {SEP:|} {TERMINAL:DROP}",
            "NORMAL | FORCE | DROP"),
        arguments("ReferenceType", "{TERMINAL:BRANCH} {SEP:|} {TERMINAL:TAG}", "BRANCH | TAG"),
        arguments(
            "HelpStatement",
            "{TERMINAL:HELP} \n"
                + "    {PRE:[} {TERMINAL:USE}\n"
                + "    {SEP:|} {TERMINAL:CONNECT}\n"
                + "    {SEP:|} {TERMINAL:CREATE} {PRE:[} {PRE:(} {TERMINAL:BRANCH} {SEP:|} {TERMINAL:TAG} {SEP:|} {TERMINAL:NAMESPACE} {POST:)} {POST:]}\n"
                + "    {SEP:|} {TERMINAL:ALTER} {PRE:[} {TERMINAL:NAMESPACE} {POST:]}\n"
                + "    {SEP:|} {TERMINAL:DROP} {PRE:[} {TERMINAL:BRANCH} {SEP:|} {TERMINAL:TAG} {SEP:|} {TERMINAL:NAMESPACE} {SEP:|} {TERMINAL:TABLE} {SEP:|} {TERMINAL:VIEW} {POST:]}\n"
                + "    {SEP:|} {TERMINAL:LIST} {PRE:[} {TERMINAL:CONTENTS} {SEP:|} {TERMINAL:REFERENCES} {POST:]}\n"
                + "    {SEP:|} {TERMINAL:SHOW} {PRE:[} {TERMINAL:LOG} {SEP:|} {TERMINAL:TABLE} {SEP:|} {TERMINAL:VIEW} {SEP:|} {TERMINAL:NAMESPACE} {SEP:|} {TERMINAL:REFERENCE} {POST:]}\n"
                + "    {SEP:|} {TERMINAL:ASSIGN} {PRE:[} {TERMINAL:BRANCH} {SEP:|} {TERMINAL:TAG} {POST:]}\n"
                + "    {SEP:|} {TERMINAL:MERGE}\n"
                + "    {SEP:|} {TERMINAL:REVERT}\n"
                + "    {SEP:|} {TERMINAL:HELP}\n"
                + "    {SEP:|} {TERMINAL:EXIT}\n"
                + "    {SEP:|} {TERMINAL:LICENSE}\n"
                + "    {POST:]}",
            "HELP \n"
                + "    [ USE\n"
                + "    | CONNECT\n"
                + "    | CREATE [ ( BRANCH | TAG | NAMESPACE ) ]\n"
                + "    | ALTER [ NAMESPACE ]\n"
                + "    | DROP [ BRANCH | TAG | NAMESPACE | TABLE | VIEW ]\n"
                + "    | LIST [ CONTENTS | REFERENCES ]\n"
                + "    | SHOW [ LOG | TABLE | VIEW | NAMESPACE | REFERENCE ]\n"
                + "    | ASSIGN [ BRANCH | TAG ]\n"
                + "    | MERGE\n"
                + "    | REVERT\n"
                + "    | HELP\n"
                + "    | EXIT\n"
                + "    | LICENSE\n"
                + "    ]"),
        arguments(
            "CreateNamespaceStatement",
            "{TERMINAL:NAMESPACE} {NON_TERMINAL:ContentKey} \n"
                + "    {PRE:[} {TERMINAL:ON} {PRE:[} {NON_TERMINAL:ReferenceType} {POST:]} {NON_TERMINAL:ExistingReference} {POST:]} \n"
                + "    {PRE:[} {TERMINAL:SET} {NON_TERMINAL:ParamKey} {TERMINAL:=} {NON_TERMINAL:Value} {PRE:{} {TERMINAL:AND} {NON_TERMINAL:ParamKey} {TERMINAL:=} {NON_TERMINAL:Value} {POST:}} {POST:]}",
            "CREATE NAMESPACE ContentKey \n"
                + "    [ ON [ ReferenceType ] ExistingReference ] \n"
                + "    [ SET ParamKey = Value { AND ParamKey = Value } ]"),
        arguments(
            "CreateReferenceStatement",
            "{NON_TERMINAL:ReferenceType} \n"
                + "    {PRE:[} {TERMINAL:IF} {TERMINAL:NOT} {TERMINAL:EXISTS} {POST:]} \n"
                + "    {NON_TERMINAL:ReferenceName} \n"
                + "    {PRE:[} {TERMINAL:FROM} {NON_TERMINAL:ExistingReference} {POST:]} \n"
                + "    {PRE:[} {TERMINAL:AT} {PRE:[} {TERMINAL:TIMESTAMP} {SEP:|} {TERMINAL:COMMIT} {POST:]} {NON_TERMINAL:TimestampOrCommit} {POST:]}",
            "CREATE ReferenceType \n"
                + "    [ IF NOT EXISTS ] \n"
                + "    ReferenceName \n"
                + "    [ FROM ExistingReference ] \n"
                + "    [ AT [ TIMESTAMP | COMMIT ] TimestampOrCommit ]"),
        arguments(
            "MergeBranchStatement",
            "{TERMINAL:MERGE} \n"
                + "    {PRE:[} {TERMINAL:DRY} {POST:]} \n"
                + "    {PRE:[} {NON_TERMINAL:ReferenceType} {POST:]} \n"
                + "    {NON_TERMINAL:ExistingReference} \n"
                + "    {PRE:[} {TERMINAL:AT} {PRE:[} {TERMINAL:TIMESTAMP} {SEP:|} {TERMINAL:COMMIT} {POST:]} {NON_TERMINAL:TimestampOrCommit} {POST:]} \n"
                + "    {PRE:[} {TERMINAL:INTO} {NON_TERMINAL:ExistingReference} {POST:]} \n"
                + "    {PRE:[} {TERMINAL:BEHAVIOR} {NON_TERMINAL:MergeBehaviorKind} {POST:]} \n"
                + "    {PRE:[} {TERMINAL:BEHAVIORS} {NON_TERMINAL:ContentKey} {TERMINAL:=} {NON_TERMINAL:MergeBehaviorKind} {PRE:{} {TERMINAL:AND} {NON_TERMINAL:ContentKey} {TERMINAL:=} {NON_TERMINAL:MergeBehaviorKind} {POST:}} {POST:]}",
            "MERGE \n"
                + "    [ DRY ] \n"
                + "    [ ReferenceType ] \n"
                + "    ExistingReference \n"
                + "    [ AT [ TIMESTAMP | COMMIT ] TimestampOrCommit ] \n"
                + "    [ INTO ExistingReference ] \n"
                + "    [ BEHAVIOR MergeBehaviorKind ] \n"
                + "    [ BEHAVIORS ContentKey = MergeBehaviorKind { AND ContentKey = MergeBehaviorKind } ]"));
  }
}

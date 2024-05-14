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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.nio.file.Paths;
import java.util.stream.Stream;
import org.congocc.core.BNFProduction;
import org.congocc.core.Grammar;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class TestSyntax {
  @ParameterizedTest
  @MethodSource
  public void syntaxTest(String production, String expected) throws Exception {
    Syntax syntax = new Syntax(Paths.get("src/main/congocc/nessie/nessie-cli.ccc"));
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

    assertThat(sb.toString()).isEqualTo(expected);
  }

  static Stream<Arguments> syntaxTest() {
    return Stream.of(
        arguments(
            "MergeBehaviorKind",
            "{TERMINAL:NORMAL} {SEP:|} {TERMINAL:FORCE} {SEP:|} {TERMINAL:DROP} {POST:\n" + "}"),
        arguments("ReferenceType", "{TERMINAL:BRANCH} {SEP:|} {TERMINAL:TAG} {POST:\n" + "}"),
        arguments(
            "HelpStatement",
            "{TERMINAL:HELP}\n"
                + "{PRE:[}\n"
                + "        {TERMINAL:USE} {SEP:|} {TERMINAL:CONNECT} {SEP:|} {TERMINAL:CREATE}\n"
                + "        {PRE:[}\n"
                + "                {TERMINAL:BRANCH} {SEP:|} {TERMINAL:TAG} {SEP:|} {TERMINAL:NAMESPACE}\n"
                + "            {POST:]}\n"
                + "        {SEP:|} {TERMINAL:ALTER}\n"
                + "        {PRE:[} {TERMINAL:NAMESPACE} {POST:]}\n"
                + "        {SEP:|} {TERMINAL:DROP}\n"
                + "        {PRE:[}\n"
                + "                {TERMINAL:BRANCH} {SEP:|} {TERMINAL:TAG} {SEP:|} {TERMINAL:NAMESPACE} {SEP:|} {TERMINAL:TABLE} {SEP:|} {TERMINAL:VIEW}\n"
                + "            {POST:]}\n"
                + "        {SEP:|} {TERMINAL:LIST}\n"
                + "        {PRE:[}\n"
                + "                {TERMINAL:CONTENTS} {SEP:|} {TERMINAL:REFERENCES}\n"
                + "            {POST:]}\n"
                + "        {SEP:|} {TERMINAL:SHOW}\n"
                + "        {PRE:[}\n"
                + "                {TERMINAL:LOG} {SEP:|} {TERMINAL:TABLE} {SEP:|} {TERMINAL:VIEW} {SEP:|} {TERMINAL:NAMESPACE} {SEP:|} {TERMINAL:REFERENCE}\n"
                + "            {POST:]}\n"
                + "        {SEP:|} {TERMINAL:ASSIGN}\n"
                + "        {PRE:[}\n"
                + "                {TERMINAL:BRANCH} {SEP:|} {TERMINAL:TAG}\n"
                + "            {POST:]}\n"
                + "        {SEP:|} {TERMINAL:MERGE} {SEP:|} {TERMINAL:HELP} {SEP:|} {TERMINAL:EXIT} {SEP:|} {TERMINAL:LICENSE}\n"
                + "    {POST:]}\n"
                + "{POST:\n"
                + "}"),
        arguments(
            "MergeBranchStatement",
            "{TERMINAL:MERGE}\n"
                + "{PRE:[} {TERMINAL:DRY} {POST:]}\n"
                + "{PRE:[} {NON_TERMINAL:ReferenceType} {POST:]}\n"
                + "{NON_TERMINAL:ExistingReference}\n"
                + "{PRE:[} {TERMINAL:AT}\n"
                + "    {PRE:[}\n"
                + "            {TERMINAL:TIMESTAMP} {SEP:|} {TERMINAL:COMMIT}\n"
                + "        {POST:]}\n"
                + "    {NON_TERMINAL:TimestampOrCommit} {POST:]}\n"
                + "{PRE:[} {TERMINAL:INTO} {NON_TERMINAL:ExistingReference} {POST:]}\n"
                + "{PRE:[} {TERMINAL:BEHAVIOR} {NON_TERMINAL:MergeBehaviorKind} {POST:]}\n"
                + "{PRE:[} {TERMINAL:BEHAVIORS} {NON_TERMINAL:ContentKey} {TERMINAL:=} {NON_TERMINAL:MergeBehaviorKind}\n"
                + "    {PRE:{} {TERMINAL:AND} {NON_TERMINAL:ContentKey} {TERMINAL:=} {NON_TERMINAL:MergeBehaviorKind} {POST:}}\n"
                + "    {POST:]}\n"
                + "{POST:\n"
                + "}"));
  }
}

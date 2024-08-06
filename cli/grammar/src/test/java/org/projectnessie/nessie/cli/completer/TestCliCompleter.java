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
package org.projectnessie.nessie.cli.completer;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.assertj.core.api.Assertions.tuple;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.projectnessie.nessie.cli.cmdspec.CommandSpec;
import org.projectnessie.nessie.cli.cmdspec.ImmutableAlterNamespaceCommandSpec;
import org.projectnessie.nessie.cli.cmdspec.ImmutableAssignReferenceCommandSpec;
import org.projectnessie.nessie.cli.cmdspec.ImmutableConnectCommandSpec;
import org.projectnessie.nessie.cli.cmdspec.ImmutableCreateNamespaceCommandSpec;
import org.projectnessie.nessie.cli.cmdspec.ImmutableCreateReferenceCommandSpec;
import org.projectnessie.nessie.cli.cmdspec.ImmutableDropContentCommandSpec;
import org.projectnessie.nessie.cli.cmdspec.ImmutableDropReferenceCommandSpec;
import org.projectnessie.nessie.cli.cmdspec.ImmutableExitCommandSpec;
import org.projectnessie.nessie.cli.cmdspec.ImmutableHelpCommandSpec;
import org.projectnessie.nessie.cli.cmdspec.ImmutableListContentsCommandSpec;
import org.projectnessie.nessie.cli.cmdspec.ImmutableListReferencesCommandSpec;
import org.projectnessie.nessie.cli.cmdspec.ImmutableMergeBranchCommandSpec;
import org.projectnessie.nessie.cli.cmdspec.ImmutableRevertContentCommandSpec;
import org.projectnessie.nessie.cli.cmdspec.ImmutableShowContentCommandSpec;
import org.projectnessie.nessie.cli.cmdspec.ImmutableShowLogCommandSpec;
import org.projectnessie.nessie.cli.cmdspec.ImmutableShowReferenceCommandSpec;
import org.projectnessie.nessie.cli.cmdspec.ImmutableUseReferenceCommandSpec;
import org.projectnessie.nessie.cli.grammar.CompletionType;
import org.projectnessie.nessie.cli.grammar.NessieCliLexer;
import org.projectnessie.nessie.cli.grammar.NessieCliParser;
import org.projectnessie.nessie.cli.grammar.Node;
import org.projectnessie.nessie.cli.grammar.Token.TokenType;
import org.projectnessie.nessie.cli.grammar.ast.Script;
import org.projectnessie.nessie.cli.grammar.ast.SingleStatement;

@ExtendWith(SoftAssertionsExtension.class)
public class TestCliCompleter {
  @InjectSoftAssertions protected SoftAssertions soft;

  @ParameterizedTest
  @MethodSource
  public void literalCompletions(
      String input,
      boolean expectedOutcome,
      String expected,
      boolean expectedQuoted,
      CompletionType expectedCompletionType) {
    CliCompleter completer =
        new CliCompleter(
            input,
            input.length(),
            TestCliCompleter::sourceParser,
            NessieCliParser::SingleStatement) {
          @Override
          protected void completeWithLiteral(
              CompletionType completionType, String preceding, String toComplete, boolean quoted) {
            soft.assertThat(tuple(completionType, toComplete, quoted))
                .isEqualTo(tuple(expectedCompletionType, expected, expectedQuoted));
          }

          @Override
          protected void tokenCandidateStartsWith(String preceding, TokenType toComplete) {
            // ignore
          }

          @Override
          protected void tokenCandidateContains(String preceding, TokenType toComplete) {
            // ignore
          }

          @Override
          protected void tokenCandidateOther(String preceding, TokenType toComplete) {
            // ignore
          }
        };

    boolean outcome = completer.tryStatement();
    soft.assertThat(outcome).isEqualTo(expectedOutcome);
  }

  static Stream<Arguments> literalCompletions() {
    return Stream.of(
        arguments("show reference f", false, "f", false, CompletionType.REFERENCE_NAME, List.of()),
        arguments("show log on f", false, "f", false, CompletionType.NONE, List.of()),
        arguments("create branch f", false, "f", false, CompletionType.NONE, List.of()),
        arguments("create branch \"f", true, "f", true, CompletionType.NONE, List.of()),
        arguments("create branch \"f\"", false, "f", true, CompletionType.NONE, List.of()),
        arguments("create branch 'f", true, "f", true, CompletionType.NONE, List.of()),
        arguments("create branch 'f'", false, "f", true, CompletionType.NONE, List.of()),
        arguments("create branch `f", true, "f", true, CompletionType.NONE, List.of()),
        arguments("create branch `f`", false, "f", true, CompletionType.NONE, List.of()),
        arguments(
            "create branch \"foo non_keyword",
            true,
            "foo non_keyword",
            true,
            CompletionType.NONE,
            List.of()),
        arguments(
            "create branch 'foo non_keyword",
            true,
            "foo non_keyword",
            true,
            CompletionType.NONE,
            List.of()),
        arguments(
            "create branch `foo non_keyword",
            true,
            "foo non_keyword",
            true,
            CompletionType.NONE,
            List.of()),
        arguments(
            "create branch \"foo if not exists",
            true,
            "foo if not exists",
            true,
            CompletionType.NONE));
  }

  @ParameterizedTest
  @MethodSource
  public void completer(
      String input,
      boolean expectedOutcome,
      List<String> expectedStartsWith,
      List<String> expectedContains,
      List<String> expectedOther,
      CompletionType expectedCompletionType,
      List<TokenType> expectedOptionalNextTokenTypes) {
    List<String> startsWith = new ArrayList<>();
    List<String> contains = new ArrayList<>();
    List<String> other = new ArrayList<>();
    AtomicReference<List<TokenType>> optionalTypes = new AtomicReference<>(List.of());
    CliCompleter completer =
        new CliCompleter(
            input,
            input.length(),
            TestCliCompleter::sourceParser,
            NessieCliParser::SingleStatement) {
          @Override
          protected void completeWithLiteral(
              CompletionType completionType, String preceding, String toComplete, boolean quoted) {
            soft.assertThat(completionType).isEqualTo(expectedCompletionType);
            // ignore literal completions in this test
            optionalTypes.set(parser().optionalNextTokenTypes());
          }

          @Override
          protected void tokenCandidateStartsWith(String preceding, TokenType toComplete) {
            startsWith.add(preceding + parser().tokenToLiteral(toComplete));
            optionalTypes.set(parser().optionalNextTokenTypes());
          }

          @Override
          protected void tokenCandidateContains(String preceding, TokenType toComplete) {
            contains.add(preceding + parser().tokenToLiteral(toComplete));
            optionalTypes.set(parser().optionalNextTokenTypes());
          }

          @Override
          protected void tokenCandidateOther(String preceding, TokenType toComplete) {
            other.add(preceding + parser().tokenToLiteral(toComplete));
            optionalTypes.set(parser().optionalNextTokenTypes());
          }
        };

    boolean outcome = completer.tryStatement();
    soft.assertThat(outcome).isEqualTo(expectedOutcome);
    soft.assertThat(optionalTypes.get()).containsExactlyElementsOf(expectedOptionalNextTokenTypes);

    soft.assertThat(startsWith)
        .describedAs("completion/startsWith")
        .containsExactlyInAnyOrderElementsOf(expectedStartsWith);
    soft.assertThat(contains)
        .describedAs("completion/contains")
        .containsExactlyInAnyOrderElementsOf(expectedContains);
    soft.assertThat(other)
        .describedAs("completion/other")
        .containsExactlyInAnyOrderElementsOf(expectedOther);
  }

  private static NessieCliParser sourceParser(String source) {
    NessieCliLexer lexer = new NessieCliLexer(source);
    return new NessieCliParser(lexer);
  }

  static Stream<Arguments> completer() {
    return Stream.of(
        arguments(
            "USE",
            true,
            List.of(),
            List.of(),
            List.of("USE BRANCH", "USE TAG", "USE REFERENCE"),
            CompletionType.NONE,
            List.of()),
        arguments(
            "REVERT",
            true,
            List.of(),
            List.of(),
            List.of("REVERT CONTENT"),
            CompletionType.NONE,
            List.of()),
        arguments(
            "REVERT CONTENT",
            true,
            List.of(),
            List.of(),
            List.of("REVERT CONTENT OF", "REVERT CONTENT DRY"),
            CompletionType.NONE,
            List.of(TokenType.DRY)),
        arguments(
            "REVERT CONTENT DRY",
            true,
            List.of(),
            List.of(),
            List.of("REVERT CONTENT DRY OF"),
            CompletionType.NONE,
            List.of()),
        arguments(
            "REVERT CONTENT OF foo ",
            true,
            List.of(),
            List.of(),
            List.of(
                "REVERT CONTENT OF foo AND",
                "REVERT CONTENT OF foo ON",
                "REVERT CONTENT OF foo TO"),
            CompletionType.NONE,
            List.of(
                TokenType.ON, TokenType.AND)), // Note: TokenType.TO is _mandatory_ (not optional)
        arguments(
            "REVERT CONTENT OF foo AND bar ",
            true,
            List.of(),
            List.of(),
            List.of(
                "REVERT CONTENT OF foo AND bar AND",
                "REVERT CONTENT OF foo AND bar ON",
                "REVERT CONTENT OF foo AND bar TO"),
            CompletionType.NONE,
            List.of(
                TokenType.ON, TokenType.AND)), // Note: TokenType.TO is _mandatory_ (not optional)
        arguments(
            "REVERT CONTENT OF foo ON ",
            true,
            List.of(),
            List.of(),
            List.of("REVERT CONTENT OF foo ON BRANCH"),
            CompletionType.REFERENCE_NAME,
            List.of(TokenType.BRANCH)),
        arguments(
            "REVERT CONTENT OF foo ON BRANCH main",
            true,
            List.of(),
            List.of(),
            List.of("REVERT CONTENT OF foo ON BRANCH main TO"),
            CompletionType.NONE,
            List.of()),
        arguments(
            "REVERT CONTENT OF foo ON BRANCH main TO",
            true,
            List.of(),
            List.of(),
            List.of("REVERT CONTENT OF foo ON BRANCH main TO STATE"),
            CompletionType.NONE,
            List.of()),
        arguments(
            "REVERT CONTENT OF foo ON BRANCH main TO STATE",
            true,
            List.of(),
            List.of(),
            List.of(
                "REVERT CONTENT OF foo ON BRANCH main TO STATE ON",
                "REVERT CONTENT OF foo ON BRANCH main TO STATE AT"),
            CompletionType.NONE,
            List.of()),
        arguments(
            "REVERT CONTENT OF foo ON BRANCH main TO STATE ON",
            true,
            List.of(),
            List.of(),
            List.of(
                "REVERT CONTENT OF foo ON BRANCH main TO STATE ON BRANCH",
                "REVERT CONTENT OF foo ON BRANCH main TO STATE ON TAG"),
            CompletionType.REFERENCE_NAME,
            List.of(TokenType.BRANCH, TokenType.TAG)),
        arguments(
            "REVERT CONTENT OF foo ON BRANCH main TO STATE ON TAG taggy ",
            false,
            List.of(),
            List.of(),
            List.of(
                "REVERT CONTENT OF foo ON BRANCH main TO STATE ON TAG taggy AT",
                "REVERT CONTENT OF foo ON BRANCH main TO STATE ON TAG taggy ALLOW"),
            CompletionType.NONE,
            List.of(TokenType.ALLOW, TokenType.AT)),
        arguments(
            "REVERT CONTENT OF foo ON BRANCH main TO STATE ON TAG taggy AT",
            true,
            List.of(),
            List.of(),
            List.of(
                "REVERT CONTENT OF foo ON BRANCH main TO STATE ON TAG taggy AT TIMESTAMP",
                "REVERT CONTENT OF foo ON BRANCH main TO STATE ON TAG taggy AT COMMIT"),
            CompletionType.NONE,
            List.of(TokenType.TIMESTAMP, TokenType.COMMIT)),
        arguments(
            "REVERT CONTENT OF foo ON BRANCH main TO STATE AT",
            true,
            List.of(),
            List.of(),
            List.of(
                "REVERT CONTENT OF foo ON BRANCH main TO STATE AT TIMESTAMP",
                "REVERT CONTENT OF foo ON BRANCH main TO STATE AT COMMIT"),
            CompletionType.NONE,
            List.of(TokenType.TIMESTAMP, TokenType.COMMIT)),
        //
        arguments(
            "CONNECT",
            true,
            List.of(),
            List.of(),
            List.of("CONNECT TO"),
            CompletionType.NONE,
            List.of()),
        arguments(
            "CONNECT TO http://127.0.0.1 ",
            false,
            List.of(),
            List.of(),
            List.of("CONNECT TO http://127.0.0.1 ON", "CONNECT TO http://127.0.0.1 USING"),
            CompletionType.NONE,
            List.of(TokenType.ON, TokenType.USING)),
        arguments(
            "CONNECT TO \"http://127.0.0.1\"",
            false,
            List.of(),
            List.of(),
            List.of("CONNECT TO \"http://127.0.0.1\" ON", "CONNECT TO \"http://127.0.0.1\" USING"),
            CompletionType.NONE,
            List.of(TokenType.ON, TokenType.USING)),
        arguments(
            "CONNECT TO http://127.0.0.1 ON ",
            true,
            List.of(),
            List.of(),
            List.of(),
            CompletionType.NONE,
            List.of()),
        arguments(
            "CONNECT TO http://127.0.0.1 ON myBranch",
            false,
            List.of(),
            List.of(),
            List.of("CONNECT TO http://127.0.0.1 ON myBranch USING"),
            CompletionType.NONE,
            List.of(TokenType.USING)),
        arguments(
            "CONNECT TO http://127.0.0.1 USING ",
            true,
            List.of(),
            List.of(),
            List.of(),
            CompletionType.CONNECT_OPTIONS,
            List.of()),
        arguments(
            "CONNECT TO http://127.0.0.1 USING abc ",
            true,
            List.of(),
            List.of(),
            List.of("CONNECT TO http://127.0.0.1 USING abc ="),
            CompletionType.NONE,
            List.of()),
        arguments(
            "CONNECT TO http://127.0.0.1 USING abc = foo ",
            false,
            List.of(),
            List.of(),
            List.of("CONNECT TO http://127.0.0.1 USING abc = foo AND"),
            CompletionType.NONE,
            List.of(TokenType.AND)),
        arguments(
            "CONNECT TO http://127.0.0.1 USING \"abc\" ",
            true,
            List.of(),
            List.of(),
            List.of("CONNECT TO http://127.0.0.1 USING \"abc\" ="),
            CompletionType.NONE,
            List.of()),
        arguments(
            "CONNECT TO http://127.0.0.1 USING \"abc\" = \"foo\" ",
            false,
            List.of(),
            List.of(),
            List.of("CONNECT TO http://127.0.0.1 USING \"abc\" = \"foo\" AND"),
            CompletionType.NONE,
            List.of(TokenType.AND)),
        arguments(
            "CONNECT TO http://127.0.0.1 USING \"abc\" = foo ",
            false,
            List.of(),
            List.of(),
            List.of("CONNECT TO http://127.0.0.1 USING \"abc\" = foo AND"),
            CompletionType.NONE,
            List.of(TokenType.AND)),
        arguments(
            "CONNECT TO http://127.0.0.1 USING \"abc\" = foo AND ",
            true,
            List.of(),
            List.of(),
            List.of(),
            CompletionType.CONNECT_OPTIONS,
            List.of()),
        arguments(
            "h",
            true,
            List.of("HELP"),
            List.of("SHOW"),
            List.of(
                "CONNECT", "USE", "DROP", "LIST", "EXIT", "MERGE", "ALTER", "ASSIGN", "CREATE",
                "REVERT"),
            CompletionType.NONE,
            List.of()),
        arguments(
            "help ",
            false,
            List.of(),
            List.of(),
            List.of(
                "help USE",
                "help CONNECT",
                "help CREATE",
                "help DROP",
                "help LIST",
                "help SHOW",
                "help ASSIGN",
                "help ALTER",
                "help MERGE",
                "help REVERT",
                "help HELP",
                "help EXIT",
                "help LICENSE"),
            CompletionType.NONE,
            List.of(
                TokenType.USE,
                TokenType.CONNECT,
                TokenType.CREATE,
                TokenType.ALTER,
                TokenType.DROP,
                TokenType.LIST,
                TokenType.SHOW,
                TokenType.ASSIGN,
                TokenType.MERGE,
                TokenType.REVERT,
                TokenType.HELP,
                TokenType.EXIT,
                TokenType.LICENSE)),
        arguments(
            "help u",
            true,
            List.of("help USE"),
            List.of(),
            List.of(
                "help CONNECT",
                "help CREATE",
                "help ALTER",
                "help DROP",
                "help LIST",
                "help SHOW",
                "help ASSIGN",
                "help MERGE",
                "help REVERT",
                "help HELP",
                "help EXIT",
                "help LICENSE"),
            CompletionType.NONE,
            List.of(
                TokenType.USE,
                TokenType.CONNECT,
                TokenType.CREATE,
                TokenType.ALTER,
                TokenType.DROP,
                TokenType.LIST,
                TokenType.SHOW,
                TokenType.ASSIGN,
                TokenType.MERGE,
                TokenType.REVERT,
                TokenType.HELP,
                TokenType.EXIT,
                TokenType.LICENSE)),
        arguments(
            "help create ",
            false,
            List.of(),
            List.of(),
            List.of("help create BRANCH", "help create TAG", "help create NAMESPACE"),
            CompletionType.NONE,
            List.of(TokenType.BRANCH, TokenType.TAG, TokenType.NAMESPACE)),
        arguments(
            "help create branch",
            false,
            List.of(),
            List.of(),
            List.of(),
            CompletionType.NONE,
            List.of()),
        arguments(
            "LIST",
            true,
            List.of(),
            List.of(),
            List.of("LIST CONTENTS", "LIST REFERENCES"),
            CompletionType.NONE,
            List.of()),
        arguments(
            "LIST CONTENTS",
            false,
            List.of(),
            List.of(),
            List.of(
                "LIST CONTENTS ON",
                "LIST CONTENTS AT",
                "LIST CONTENTS FILTER",
                "LIST CONTENTS STARTING",
                "LIST CONTENTS CONTAINING"),
            CompletionType.NONE,
            List.of(
                TokenType.ON,
                TokenType.AT,
                TokenType.FILTER,
                TokenType.STARTING,
                TokenType.CONTAINING)),
        arguments(
            "LIST CONTENTS ON",
            true,
            List.of(),
            List.of(),
            List.of("LIST CONTENTS ON BRANCH", "LIST CONTENTS ON TAG"),
            CompletionType.REFERENCE_NAME,
            List.of(TokenType.BRANCH, TokenType.TAG)),
        arguments(
            "LIST CONTENTS STARTING ",
            true,
            List.of(),
            List.of(),
            List.of("LIST CONTENTS STARTING WITH"),
            CompletionType.NONE,
            List.of()),
        arguments(
            "LIST CONTENTS AT ",
            true,
            List.of(),
            List.of(),
            List.of("LIST CONTENTS AT TIMESTAMP", "LIST CONTENTS AT COMMIT"),
            CompletionType.NONE,
            List.of(TokenType.TIMESTAMP, TokenType.COMMIT)),
        arguments(
            "LIST REFERENCES",
            false,
            List.of(),
            List.of(),
            List.of(
                "LIST REFERENCES FILTER",
                "LIST REFERENCES STARTING",
                "LIST REFERENCES CONTAINING",
                "LIST REFERENCES IN"),
            CompletionType.NONE,
            List.of(TokenType.FILTER, TokenType.STARTING, TokenType.CONTAINING, TokenType.IN)),
        arguments(
            "LIST REFERENCES STARTING ",
            true,
            List.of(),
            List.of(),
            List.of("LIST REFERENCES STARTING WITH"),
            CompletionType.NONE,
            List.of()),
        arguments(
            "show",
            true,
            List.of(),
            List.of(),
            List.of("show LOG", "show TABLE", "show VIEW", "show NAMESPACE", "show REFERENCE"),
            CompletionType.NONE,
            List.of()),
        arguments(
            "show ",
            true,
            List.of(),
            List.of(),
            List.of("show LOG", "show TABLE", "show VIEW", "show NAMESPACE", "show REFERENCE"),
            CompletionType.NONE,
            List.of()),
        arguments(
            "show l",
            true,
            List.of("show LOG"),
            List.of("show TABLE"),
            List.of("show VIEW", "show NAMESPACE", "show REFERENCE"),
            CompletionType.NONE,
            List.of()),
        arguments(
            "show log",
            false,
            List.of(),
            List.of(),
            List.of("show log ON", "show log AT", "show log LIMIT", "show log IN"),
            CompletionType.NONE,
            List.of(TokenType.ON, TokenType.AT, TokenType.LIMIT, TokenType.IN)),
        arguments(
            "show table ",
            true,
            List.of(),
            List.of(),
            List.of("show table ON", "show table AT"),
            CompletionType.NONE,
            List.of(TokenType.ON, TokenType.AT)),
        arguments(
            "show table on",
            true,
            List.of(),
            List.of(),
            List.of("show table on BRANCH", "show table on TAG"),
            CompletionType.REFERENCE_NAME,
            List.of(TokenType.BRANCH, TokenType.TAG)),
        arguments(
            "show table on BRANCH",
            true,
            List.of(),
            List.of(),
            List.of(),
            CompletionType.REFERENCE_NAME,
            List.of()),
        arguments(
            "show table on branch foo at",
            true,
            List.of(),
            List.of(),
            List.of("show table on branch foo at TIMESTAMP", "show table on branch foo at COMMIT"),
            CompletionType.NONE,
            List.of(TokenType.TIMESTAMP, TokenType.COMMIT)),
        arguments(
            "show r",
            true,
            List.of("show REFERENCE"),
            List.of(),
            List.of("show LOG", "show VIEW", "show TABLE", "show NAMESPACE"),
            CompletionType.REFERENCE_NAME,
            List.of()),
        arguments(
            "create branch f",
            false,
            List.of(),
            List.of(),
            List.of("create branch f IN", "create branch f FROM", "create branch f AT"),
            CompletionType.NONE,
            List.of(TokenType.IN, TokenType.FROM, TokenType.AT)),
        arguments(
            "create branch",
            true,
            List.of(),
            List.of(),
            List.of("create branch IF"),
            CompletionType.NONE,
            List.of(TokenType.IF)),
        arguments(
            "create branch \"f",
            true,
            List.of(),
            List.of(),
            List.of("create branch IF"),
            CompletionType.NONE,
            List.of(TokenType.IF)),
        arguments(
            "create branch \"f\"",
            false,
            List.of(),
            List.of(),
            List.of("create branch \"f\" IN", "create branch \"f\" FROM", "create branch \"f\" AT"),
            CompletionType.NONE,
            List.of(TokenType.IN, TokenType.FROM, TokenType.AT)),
        arguments(
            "create branch \"foo non_keyword",
            true,
            List.of(),
            List.of(),
            List.of("create branch IF"),
            CompletionType.NONE,
            List.of(TokenType.IF)),
        arguments(
            "create branch \"foo if not exists",
            true,
            List.of(),
            List.of(),
            List.of("create branch IF"),
            CompletionType.NONE,
            List.of(TokenType.IF)),
        arguments(
            "",
            true,
            List.of(),
            List.of(),
            List.of(
                "CONNECT", "REVERT", "USE", "DROP", "LIST", "SHOW", "HELP", "ALTER", "EXIT",
                "MERGE", "ASSIGN", "CREATE"),
            CompletionType.NONE,
            List.of()),
        arguments(
            " ",
            true,
            List.of(),
            List.of(),
            List.of(
                " CONNECT",
                " REVERT",
                " USE",
                " DROP",
                " LIST",
                " SHOW",
                " HELP",
                " EXIT",
                " ALTER",
                " MERGE",
                " ASSIGN",
                " CREATE"),
            CompletionType.NONE,
            List.of()),
        arguments(
            "C",
            true,
            List.of("CONNECT", "CREATE"),
            List.of(),
            List.of(
                "USE", "DROP", "LIST", "SHOW", "HELP", "ALTER", "EXIT", "MERGE", "ASSIGN",
                "REVERT"),
            CompletionType.NONE,
            List.of()),
        arguments(
            "c",
            true,
            List.of("CONNECT", "CREATE"),
            List.of(),
            List.of(
                "USE", "DROP", "LIST", "SHOW", "HELP", "ALTER", "EXIT", "MERGE", "ASSIGN",
                "REVERT"),
            CompletionType.NONE,
            List.of()),
        arguments(
            " C",
            true,
            List.of(" CONNECT", " CREATE"),
            List.of(),
            List.of(
                " USE", " DROP", " LIST", " SHOW", " HELP", " ALTER", " EXIT", " MERGE", " ASSIGN",
                " REVERT"),
            CompletionType.NONE,
            List.of()),
        arguments(
            " c",
            true,
            List.of(" CONNECT", " CREATE"),
            List.of(),
            List.of(
                " USE", " DROP", " LIST", " SHOW", " HELP", " ALTER", " EXIT", " MERGE", " ASSIGN",
                " REVERT"),
            CompletionType.NONE,
            List.of()),
        arguments(
            "X",
            true,
            List.of(),
            List.of("EXIT"),
            List.of(
                "CONNECT", "USE", "DROP", "LIST", "SHOW", "HELP", "ALTER", "MERGE", "ASSIGN",
                "CREATE", "REVERT"),
            CompletionType.NONE,
            List.of()),
        arguments(
            "CREATE",
            true,
            List.of(),
            List.of(),
            List.of("CREATE NAMESPACE", "CREATE BRANCH", "CREATE TAG"),
            CompletionType.NONE,
            List.of()),
        arguments(
            "CREATE ",
            true,
            List.of(),
            List.of(),
            List.of("CREATE NAMESPACE", "CREATE BRANCH", "CREATE TAG"),
            CompletionType.NONE,
            List.of()),
        arguments(
            "create ",
            true,
            List.of(),
            List.of(),
            List.of("create NAMESPACE", "create BRANCH", "create TAG"),
            CompletionType.NONE,
            List.of()),
        arguments(
            "cReAtE ",
            true,
            List.of(),
            List.of(),
            List.of("cReAtE NAMESPACE", "cReAtE BRANCH", "cReAtE TAG"),
            CompletionType.NONE,
            List.of()),
        arguments(
            "CREATE B",
            true,
            List.of("CREATE BRANCH"),
            List.of(),
            List.of("CREATE NAMESPACE", "CREATE TAG"),
            CompletionType.NONE,
            List.of()),
        arguments(
            "create b",
            true,
            List.of("create BRANCH"),
            List.of(),
            List.of("create NAMESPACE", "create TAG"),
            CompletionType.NONE,
            List.of()),
        arguments(
            "CREATE X",
            true,
            List.of(),
            List.of(),
            List.of("CREATE NAMESPACE", "CREATE BRANCH", "CREATE TAG"),
            CompletionType.NONE,
            List.of()),
        arguments(
            "CREATE R",
            true,
            List.of(),
            List.of("CREATE BRANCH"),
            List.of("CREATE NAMESPACE", "CREATE TAG"),
            CompletionType.NONE,
            List.of()),
        arguments(
            "CREATE A",
            true,
            List.of(),
            List.of("CREATE NAMESPACE", "CREATE BRANCH", "CREATE TAG"),
            List.of(),
            CompletionType.NONE,
            List.of()),
        arguments(
            "CREATE BRANCH",
            true,
            List.of(),
            List.of(),
            List.of("CREATE BRANCH IF"),
            CompletionType.NONE,
            List.of(TokenType.IF)),
        arguments(
            "CREATE BRANCH ",
            true,
            List.of(),
            List.of(),
            List.of("CREATE BRANCH IF"),
            CompletionType.NONE,
            List.of(TokenType.IF)),
        arguments(
            "DROP BRANCH",
            true,
            List.of(),
            List.of(),
            List.of("DROP BRANCH IF"),
            CompletionType.REFERENCE_NAME,
            List.of(TokenType.IF)),
        arguments(
            "DROP BRANCH ",
            true,
            List.of(),
            List.of(),
            List.of("DROP BRANCH IF"),
            CompletionType.REFERENCE_NAME,
            List.of(TokenType.IF)),
        arguments(
            "DROP BRANCH IF",
            true,
            List.of(),
            List.of(),
            List.of("DROP BRANCH IF EXISTS"),
            CompletionType.REFERENCE_NAME,
            List.of()),
        arguments(
            "DROP BRANCH IF ",
            true,
            List.of(),
            List.of(),
            List.of("DROP BRANCH IF EXISTS"),
            CompletionType.REFERENCE_NAME,
            List.of()),
        arguments(
            "USE\n BRANCH\n main\n ",
            false,
            List.of(),
            List.of(),
            List.of("USE\n BRANCH\n main\n AT", "USE\n BRANCH\n main\n IN"),
            // Must not reset the completion type in the parser, so this value is expected here
            CompletionType.NONE,
            List.of(TokenType.AT, TokenType.IN)),
        arguments(
            "USE\n BRANCH\n \"main\"\n",
            false,
            List.of(),
            List.of(),
            List.of("USE\n BRANCH\n \"main\"\nAT", "USE\n BRANCH\n \"main\"\nIN"),
            // Must not reset the completion type in the parser, so this value is expected here
            CompletionType.NONE,
            List.of(TokenType.AT, TokenType.IN)),
        arguments(
            "CREATE BRANCH IF",
            true,
            List.of(),
            List.of(),
            List.of("CREATE BRANCH IF NOT"),
            CompletionType.NONE,
            List.of()),
        arguments(
            "CREATE BRANCH IF ",
            true,
            List.of(),
            List.of(),
            List.of("CREATE BRANCH IF NOT"),
            CompletionType.NONE,
            List.of()),
        arguments(
            "CREATE BRANCH IF NOT",
            true,
            List.of(),
            List.of(),
            List.of("CREATE BRANCH IF NOT EXISTS"),
            CompletionType.NONE,
            List.of()),
        arguments(
            "CREATE BRANCH IF NOT ",
            true,
            List.of(),
            List.of(),
            List.of("CREATE BRANCH IF NOT EXISTS"),
            CompletionType.NONE,
            List.of()),
        arguments(
            "CREATE BRANCH IF NOT EXISTS",
            true,
            List.of(),
            List.of(),
            List.of(),
            // Must not reset the completion type in the parser, so this value is expected here
            CompletionType.NONE,
            List.of()),
        arguments(
            "DROP NAMESPACE ",
            true,
            List.of(),
            List.of(),
            List.of(),
            CompletionType.CONTENT_KEY,
            List.of()),
        arguments(
            "ALTER NAMESPACE ",
            true,
            List.of(),
            List.of(),
            List.of(),
            CompletionType.CONTENT_KEY,
            List.of()),
        arguments(
            "DROP TABLE ",
            true,
            List.of(),
            List.of(),
            List.of(),
            CompletionType.CONTENT_KEY,
            List.of()),
        arguments(
            "ASSIGN",
            true,
            List.of(),
            List.of(),
            List.of("ASSIGN BRANCH", "ASSIGN TAG"),
            CompletionType.NONE,
            List.of()),
        arguments(
            "MERGE",
            true,
            List.of(),
            List.of(),
            List.of("MERGE BRANCH", "MERGE TAG", "MERGE DRY"),
            CompletionType.REFERENCE_NAME,
            List.of(TokenType.BRANCH, TokenType.TAG, TokenType.DRY)),
        arguments(
            "MERGE DRY",
            true,
            List.of(),
            List.of(),
            List.of("MERGE DRY BRANCH", "MERGE DRY TAG"),
            CompletionType.REFERENCE_NAME,
            List.of(TokenType.BRANCH, TokenType.TAG)),
        arguments(
            "MERGE foop",
            false,
            List.of(),
            List.of(),
            List.of(
                "MERGE foop AT",
                "MERGE foop INTO",
                "MERGE foop BEHAVIOR",
                "MERGE foop BEHAVIORS",
                "MERGE foop IN"),
            CompletionType.NONE,
            List.of(
                TokenType.AT,
                TokenType.INTO,
                TokenType.BEHAVIOR,
                TokenType.BEHAVIORS,
                TokenType.IN)),
        arguments(
            "MERGE foop AT daedbeef",
            false,
            List.of(),
            List.of(),
            List.of(
                "MERGE foop AT daedbeef INTO",
                "MERGE foop AT daedbeef BEHAVIOR",
                "MERGE foop AT daedbeef BEHAVIORS",
                "MERGE foop AT daedbeef IN"),
            CompletionType.NONE,
            List.of(TokenType.INTO, TokenType.BEHAVIOR, TokenType.BEHAVIORS, TokenType.IN)),
        arguments(
            "MERGE foop INTO meep",
            false,
            List.of(),
            List.of(),
            List.of(
                "MERGE foop INTO meep BEHAVIOR",
                "MERGE foop INTO meep BEHAVIORS",
                "MERGE foop INTO meep IN"),
            CompletionType.NONE,
            List.of(TokenType.BEHAVIOR, TokenType.BEHAVIORS, TokenType.IN)),
        arguments(
            "MERGE foop BEHAVIOR",
            true,
            List.of(),
            List.of(),
            List.of(
                "MERGE foop BEHAVIOR NORMAL",
                "MERGE foop BEHAVIOR FORCE",
                "MERGE foop BEHAVIOR DROP"),
            CompletionType.NONE,
            List.of()),
        arguments(
            "MERGE foop BEHAVIOR NORMAL",
            false,
            List.of(),
            List.of(),
            List.of("MERGE foop BEHAVIOR NORMAL BEHAVIORS", "MERGE foop BEHAVIOR NORMAL IN"),
            CompletionType.NONE,
            List.of(TokenType.BEHAVIORS, TokenType.IN)),
        arguments(
            "MERGE foop BEHAVIOR DROP BEHAVIORS",
            true,
            List.of(),
            List.of(),
            List.of(),
            CompletionType.NONE,
            List.of()),
        arguments(
            "MERGE foop BEHAVIOR DROP BEHAVIORS xyz",
            true,
            List.of(),
            List.of(),
            List.of("MERGE foop BEHAVIOR DROP BEHAVIORS xyz ="),
            CompletionType.NONE,
            List.of()),
        arguments(
            "MERGE foop BEHAVIOR DROP BEHAVIORS xyz =",
            true,
            List.of(),
            List.of(),
            List.of(
                "MERGE foop BEHAVIOR DROP BEHAVIORS xyz = NORMAL",
                "MERGE foop BEHAVIOR DROP BEHAVIORS xyz = FORCE",
                "MERGE foop BEHAVIOR DROP BEHAVIORS xyz = DROP"),
            CompletionType.NONE,
            List.of()),
        arguments(
            "MERGE foop BEHAVIOR DROP BEHAVIORS xyz = FORCE",
            false,
            List.of(),
            List.of(),
            List.of(
                "MERGE foop BEHAVIOR DROP BEHAVIORS xyz = FORCE AND",
                "MERGE foop BEHAVIOR DROP BEHAVIORS xyz = FORCE IN"),
            CompletionType.NONE,
            List.of(TokenType.AND, TokenType.IN)),
        arguments(
            "MERGE foop BEHAVIOR DROP BEHAVIORS xyz = FORCE AND",
            true,
            List.of(),
            List.of(),
            List.of(),
            CompletionType.NONE,
            List.of()),
        arguments(
            "MERGE foop BEHAVIOR DROP BEHAVIORS xyz = FORCE AND abc",
            true,
            List.of(),
            List.of(),
            List.of("MERGE foop BEHAVIOR DROP BEHAVIORS xyz = FORCE AND abc ="),
            CompletionType.NONE,
            List.of()),
        arguments(
            "MERGE foop BEHAVIOR DROP BEHAVIORS xyz = FORCE AND abc =",
            true,
            List.of(),
            List.of(),
            List.of(
                "MERGE foop BEHAVIOR DROP BEHAVIORS xyz = FORCE AND abc = NORMAL",
                "MERGE foop BEHAVIOR DROP BEHAVIORS xyz = FORCE AND abc = FORCE",
                "MERGE foop BEHAVIOR DROP BEHAVIORS xyz = FORCE AND abc = DROP"),
            CompletionType.NONE,
            List.of()));
  }

  @ParameterizedTest
  @MethodSource
  public void parse(String input, List<? extends CommandSpec> expectedSpecs) {
    soft.assertThatCode(
            () -> {
              NessieCliLexer lexer = new NessieCliLexer(input);
              NessieCliParser parser = new NessieCliParser(lexer);

              parser.Script();
              Node node = parser.rootNode();
              Script script = (Script) node;
              List<CommandSpec> commandSpecs =
                  script.getCommandSpecs().stream()
                      .map(TestCliCompleter::asImmutable)
                      .collect(Collectors.toList());
              soft.assertThat(commandSpecs).containsExactlyElementsOf(expectedSpecs);
            })
        .doesNotThrowAnyException();

    if (expectedSpecs.size() == 1) {
      soft.assertThatCode(
              () -> {
                NessieCliLexer lexer = new NessieCliLexer(input);
                NessieCliParser parser = new NessieCliParser(lexer);

                parser.SingleStatement();
                Node node = parser.rootNode();
                SingleStatement singleStatement = (SingleStatement) node;
                CommandSpec commandSpec = singleStatement.getCommandSpec();
                commandSpec = asImmutable(commandSpec);
                soft.assertThat(commandSpec).isEqualTo(expectedSpecs.get(0));
              })
          .doesNotThrowAnyException();
    }
  }

  static Stream<Arguments> parse() {
    return Stream.of(
        arguments(
            "MERGE BRANCH foo IN nessie",
            List.of(
                ImmutableMergeBranchCommandSpec.builder()
                    .ref("foo")
                    .refType("BRANCH")
                    .inCatalog("nessie")
                    .build())),
        arguments(
            "/* some comment here */ USE // blah\nREFERENCE -- meep\ntestComments IN nessie -- and there",
            List.of(
                ImmutableUseReferenceCommandSpec.builder()
                    .inCatalog("nessie")
                    .ref("testComments")
                    .build())),
        arguments(
            "/* some comment here */ USE /* comment */ REFERENCE testComments IN nessie // and there",
            List.of(
                ImmutableUseReferenceCommandSpec.builder()
                    .inCatalog("nessie")
                    .ref("testComments")
                    .build())),
        arguments(
            "-- leading \n CREATE BRANCH /* inner */ IF NOT /* inner \ninner \ninner \n */ EXISTS testComments_other IN nessie",
            List.of(
                ImmutableCreateReferenceCommandSpec.builder()
                    .inCatalog("nessie")
                    .ref("testComments_other")
                    .refType("BRANCH")
                    .isConditional(true)
                    .build())),
        arguments(
            " -- leading \n -- leading \n-- leading \n CREATE BRANCH IF NOT EXISTS testComments_other IN nessie",
            List.of(
                ImmutableCreateReferenceCommandSpec.builder()
                    .inCatalog("nessie")
                    .ref("testComments_other")
                    .refType("BRANCH")
                    .isConditional(true)
                    .build())),
        arguments(
            "USE REFERENCE throwWhenUseShowReferencesAtTimestampWithoutTimeZone AT `2024-08-02T10:58:34.486427033` IN nessie",
            List.of(
                ImmutableUseReferenceCommandSpec.builder()
                    .inCatalog("nessie")
                    .ref("throwWhenUseShowReferencesAtTimestampWithoutTimeZone")
                    .refTimestampOrHash("2024-08-02T10:58:34.486427033")
                    .build())),
        arguments(
            "REVERT CONTENT OF foo AND bar AND \"baz.blah\" TO STATE AT COMMIT deadbeef",
            List.of(
                ImmutableRevertContentCommandSpec.builder()
                    .addContentKeys("foo", "bar", "baz.blah")
                    .sourceRefTimestampOrHash("deadbeef")
                    .build())),
        arguments(
            "REVERT CONTENT DRY OF foo AND `bar` AND 'baz.blah' TO STATE AT COMMIT deadbeef ALLOW DELETES",
            List.of(
                ImmutableRevertContentCommandSpec.builder()
                    .isDryRun(true)
                    .isAllowDeletes(true)
                    .addContentKeys("foo", "bar", "baz.blah")
                    .sourceRefTimestampOrHash("deadbeef")
                    .build())),
        arguments(
            "REVERT CONTENT DRY OF foo AND bar AND \"baz.blah\" ON BRANCH mybranch TO STATE ON TAG taggy AT COMMIT deadbeef",
            List.of(
                ImmutableRevertContentCommandSpec.builder()
                    .isDryRun(true)
                    .refType("BRANCH")
                    .ref("mybranch")
                    .addContentKeys("foo", "bar", "baz.blah")
                    .sourceRefType("TAG")
                    .sourceRef("taggy")
                    .sourceRefTimestampOrHash("deadbeef")
                    .build())),
        arguments(
            "CONNECT TO \"http://foo.bar:1234/api/v2\"",
            List.of(
                ImmutableConnectCommandSpec.builder().uri("http://foo.bar:1234/api/v2").build())),
        arguments(
            "CONNECT TO \"http://foo.bar:1234/api/v2\" ON myBranch",
            List.of(
                ImmutableConnectCommandSpec.builder()
                    .uri("http://foo.bar:1234/api/v2")
                    .initialReference("myBranch")
                    .build())),
        arguments(
            "CONNECT TO https://foo.bar/x/y/zapi/v2 USING a=b AND c=d",
            List.of(
                ImmutableConnectCommandSpec.builder()
                    .uri("https://foo.bar/x/y/zapi/v2")
                    .putParameter("a", "b")
                    .putParameter("c", "d")
                    .build())),
        arguments(
            "CONNECT TO https://foo.bar/x/y/zapi/v2 ON myBranch USING a=b AND c=d",
            List.of(
                ImmutableConnectCommandSpec.builder()
                    .uri("https://foo.bar/x/y/zapi/v2")
                    .putParameter("a", "b")
                    .putParameter("c", "d")
                    .initialReference("myBranch")
                    .build())),
        arguments("HELP", List.of(ImmutableHelpCommandSpec.builder().build())),
        arguments(
            "HELP; EXIT;",
            List.of(
                ImmutableHelpCommandSpec.builder().build(),
                ImmutableExitCommandSpec.builder().build())),
        // TODO move those to a separate test
        // arguments("HELP create branch;", List.of(ImmutableHelpCommandSpec.builder().build())),
        // arguments("HELP use;", List.of(ImmutableHelpCommandSpec.builder().build())),
        // arguments("HELP create;", List.of(ImmutableHelpCommandSpec.builder().build())),
        arguments(
            "list contents;",
            List.of(ImmutableListContentsCommandSpec.of(null, null, null, null, null, null, null))),
        arguments(
            "list contents on baz filter bar;",
            List.of(
                ImmutableListContentsCommandSpec.of(null, null, "baz", null, "bar", null, null))),
        arguments(
            "list contents on baz at 2024-04-26T10:31:05.271094723Z starting with \"foo\" containing bar;",
            List.of(
                ImmutableListContentsCommandSpec.of(
                    null, null, "baz", "2024-04-26T10:31:05.271094723Z", null, "foo", "bar"))),
        arguments(
            "list references;",
            List.of(ImmutableListReferencesCommandSpec.of(null, null, null, null, null))),
        arguments(
            "list references starting with \"foo\" containing bar;",
            List.of(ImmutableListReferencesCommandSpec.of(null, null, null, "foo", "bar"))),
        arguments(
            "list references filter \"foo\";",
            List.of(ImmutableListReferencesCommandSpec.of(null, null, "foo", null, null))),
        arguments(
            "show log", List.of(ImmutableShowLogCommandSpec.of(null, null, null, null, null))),
        arguments(
            "show log on bar;",
            List.of(ImmutableShowLogCommandSpec.of(null, null, "bar", null, null))),
        arguments(
            "show log on bar at deadbeef limit 42;",
            List.of(ImmutableShowLogCommandSpec.of(null, null, "bar", "deadbeef", 42))),
        arguments(
            "show table meep",
            List.of(ImmutableShowContentCommandSpec.of(null, null, "TABLE", null, null, "meep"))),
        arguments(
            "show view on foo meep;",
            List.of(ImmutableShowContentCommandSpec.of(null, null, "VIEW", "foo", null, "meep"))),
        arguments(
            "show namespace on foo at deadbeef meep;",
            List.of(
                ImmutableShowContentCommandSpec.of(
                    null, null, "NAMESPACE", "foo", "deadbeef", "meep"))),
        arguments(
            "show reference",
            List.of(ImmutableShowReferenceCommandSpec.of(null, null, null, null))),
        arguments(
            "show reference bar;",
            List.of(ImmutableShowReferenceCommandSpec.of(null, null, "bar", null))),
        arguments(
            "cReAtE bRaNcH \"foo\";",
            List.of(
                ImmutableCreateReferenceCommandSpec.of(
                    null, null, "BRANCH", "foo", null, null, false))),
        arguments(
            "use bRaNcH \"foo\";",
            List.of(ImmutableUseReferenceCommandSpec.of(null, null, "BRANCH", "foo", null))),
        arguments(
            "DROP TAG bar;",
            List.of(ImmutableDropReferenceCommandSpec.of(null, null, "TAG", "bar", false))),
        arguments(
            "CREATE BRANCH if not exists foo; DROP TAG bar;",
            List.of(
                ImmutableCreateReferenceCommandSpec.of(
                    null, null, "BRANCH", "foo", null, null, true),
                ImmutableDropReferenceCommandSpec.of(null, null, "TAG", "bar", false))),
        arguments(
            "create branch foo; drop tag bar;",
            List.of(
                ImmutableCreateReferenceCommandSpec.of(
                    null, null, "BRANCH", "foo", null, null, false),
                ImmutableDropReferenceCommandSpec.of(null, null, "TAG", "bar", false))),
        //
        arguments(
            "create namespace foo.bar.baz;",
            List.of(
                ImmutableCreateNamespaceCommandSpec.of(null, null, "foo.bar.baz", null, Map.of()))),
        arguments(
            "create namespace foo.bar.baz on blah;",
            List.of(
                ImmutableCreateNamespaceCommandSpec.of(
                    null, null, "foo.bar.baz", "blah", Map.of()))),
        arguments(
            "create namespace foo.bar.baz set x = y;",
            List.of(
                ImmutableCreateNamespaceCommandSpec.of(
                    null, null, "foo.bar.baz", null, Map.of("x", "y")))),
        arguments(
            "create namespace foo.bar.baz on blah set x = y;",
            List.of(
                ImmutableCreateNamespaceCommandSpec.of(
                    null, null, "foo.bar.baz", "blah", Map.of("x", "y")))),
        arguments(
            "create namespace foo.bar.baz on blah set x = y and foo = meep;",
            List.of(
                ImmutableCreateNamespaceCommandSpec.of(
                    null, null, "foo.bar.baz", "blah", Map.of("x", "y", "foo", "meep")))),
        arguments(
            "alter namespace foo.bar.baz;",
            List.of(
                ImmutableAlterNamespaceCommandSpec.of(
                    null, null, "foo.bar.baz", null, Map.of(), Set.of()))),
        arguments(
            "alter namespace foo.bar.baz on blah;",
            List.of(
                ImmutableAlterNamespaceCommandSpec.of(
                    null, null, "foo.bar.baz", "blah", Map.of(), Set.of()))),
        arguments(
            "alter namespace foo.bar.baz set x = y;",
            List.of(
                ImmutableAlterNamespaceCommandSpec.of(
                    null, null, "foo.bar.baz", null, Map.of("x", "y"), Set.of()))),
        arguments(
            "alter namespace foo.bar.baz on blah set x = y;",
            List.of(
                ImmutableAlterNamespaceCommandSpec.of(
                    null, null, "foo.bar.baz", "blah", Map.of("x", "y"), Set.of()))),
        arguments(
            "alter namespace foo.bar.baz remove x;",
            List.of(
                ImmutableAlterNamespaceCommandSpec.of(
                    null, null, "foo.bar.baz", null, Map.of(), Set.of("x")))),
        arguments(
            "alter namespace foo.bar.baz on blah remove x;",
            List.of(
                ImmutableAlterNamespaceCommandSpec.of(
                    null, null, "foo.bar.baz", "blah", Map.of(), Set.of("x")))),
        arguments(
            "alter namespace foo.bar.baz set x = y remove a;",
            List.of(
                ImmutableAlterNamespaceCommandSpec.of(
                    null, null, "foo.bar.baz", null, Map.of("x", "y"), Set.of("a")))),
        arguments(
            "alter namespace foo.bar.baz on blah set x = y remove a;",
            List.of(
                ImmutableAlterNamespaceCommandSpec.of(
                    null, null, "foo.bar.baz", "blah", Map.of("x", "y"), Set.of("a")))),
        arguments(
            "alter namespace foo.bar.baz on blah set x = y and y = z remove a and b and c;",
            List.of(
                ImmutableAlterNamespaceCommandSpec.of(
                    null,
                    null,
                    "foo.bar.baz",
                    "blah",
                    Map.of("x", "y", "y", "z"),
                    Set.of("a", "b", "c")))),
        arguments(
            "drop namespace foo.bar.baz;",
            List.of(
                ImmutableDropContentCommandSpec.of(null, null, "NAMESPACE", "foo.bar.baz", null))),
        arguments(
            "drop namespace foo.bar.baz on blah;",
            List.of(
                ImmutableDropContentCommandSpec.of(
                    null, null, "NAMESPACE", "foo.bar.baz", "blah"))),
        // ,
        arguments(
            "drop table foo.bar.baz;",
            List.of(ImmutableDropContentCommandSpec.of(null, null, "TABLE", "foo.bar.baz", null))),
        arguments(
            "drop view foo.bar.baz on blah;",
            List.of(ImmutableDropContentCommandSpec.of(null, null, "VIEW", "foo.bar.baz", "blah"))),
        //
        arguments(
            "merge branch that_branch at deadbeef;",
            List.of(
                ImmutableMergeBranchCommandSpec.of(
                    null,
                    null,
                    false,
                    "BRANCH",
                    "that_branch",
                    "deadbeef",
                    null,
                    null,
                    emptyMap()))),
        arguments(
            "merge dry tag that_branch into foo_baz behavior force;",
            List.of(
                ImmutableMergeBranchCommandSpec.of(
                    null, null, true, "TAG", "that_branch", null, "foo_baz", "FORCE", emptyMap()))),
        arguments(
            "merge dry tag my_tag into foo_baz behaviors ns.key = force;",
            List.of(
                ImmutableMergeBranchCommandSpec.of(
                    null,
                    null,
                    true,
                    "TAG",
                    "my_tag",
                    null,
                    "foo_baz",
                    null,
                    singletonMap("ns.key", "FORCE")))),
        arguments(
            "merge dry branch that_branch behavior drop behaviors ns.key = force and ns.foo = normal;",
            List.of(
                ImmutableMergeBranchCommandSpec.of(
                    null,
                    null,
                    true,
                    "BRANCH",
                    "that_branch",
                    null,
                    null,
                    "DROP",
                    ImmutableMap.of("ns.key", "FORCE", "ns.foo", "NORMAL")))),
        arguments(
            "merge dry branch that_branch at cafebabe into meep behavior drop behaviors ns.key = force and ns.foo = normal;",
            List.of(
                ImmutableMergeBranchCommandSpec.of(
                    null,
                    null,
                    true,
                    "BRANCH",
                    "that_branch",
                    "cafebabe",
                    "meep",
                    "DROP",
                    ImmutableMap.of("ns.key", "FORCE", "ns.foo", "NORMAL"))))
        //
        );
  }

  @SuppressWarnings("unchecked")
  static <T extends CommandSpec> T asImmutable(T parsedSpec) {

    switch (parsedSpec.commandType()) {
      case HELP:
        return (T) ImmutableHelpCommandSpec.builder().from(parsedSpec).sourceNode(null).build();
      case EXIT:
        return (T) ImmutableExitCommandSpec.builder().build();
      case CONNECT:
        return (T) ImmutableConnectCommandSpec.builder().from(parsedSpec).sourceNode(null).build();
      case ASSIGN_REFERENCE:
        return (T)
            ImmutableAssignReferenceCommandSpec.builder().from(parsedSpec).sourceNode(null).build();
      case USE_REFERENCE:
        return (T)
            ImmutableUseReferenceCommandSpec.builder().from(parsedSpec).sourceNode(null).build();
      case LIST_CONTENTS:
        return (T)
            ImmutableListContentsCommandSpec.builder().from(parsedSpec).sourceNode(null).build();
      case LIST_REFERENCES:
        return (T)
            ImmutableListReferencesCommandSpec.builder().from(parsedSpec).sourceNode(null).build();
      case SHOW_REFERENCE:
        return (T)
            ImmutableShowReferenceCommandSpec.builder().from(parsedSpec).sourceNode(null).build();
      case SHOW_CONTENT:
        return (T)
            ImmutableShowContentCommandSpec.builder().from(parsedSpec).sourceNode(null).build();
      case SHOW_LOG:
        return (T) ImmutableShowLogCommandSpec.builder().from(parsedSpec).sourceNode(null).build();
      case REVERT_CONTENT:
        return (T)
            ImmutableRevertContentCommandSpec.builder().from(parsedSpec).sourceNode(null).build();
      case DROP_REFERENCE:
        return (T)
            ImmutableDropReferenceCommandSpec.builder().from(parsedSpec).sourceNode(null).build();
      case CREATE_REFERENCE:
        return (T)
            ImmutableCreateReferenceCommandSpec.builder().from(parsedSpec).sourceNode(null).build();
      case CREATE_NAMESPACE:
        return (T)
            ImmutableCreateNamespaceCommandSpec.builder().from(parsedSpec).sourceNode(null).build();
      case ALTER_NAMESPACE:
        return (T)
            ImmutableAlterNamespaceCommandSpec.builder().from(parsedSpec).sourceNode(null).build();
      case DROP_CONTENT:
        return (T)
            ImmutableDropContentCommandSpec.builder().from(parsedSpec).sourceNode(null).build();
      case MERGE_BRANCH:
        return (T)
            ImmutableMergeBranchCommandSpec.builder().from(parsedSpec).sourceNode(null).build();
      default:
        throw new IllegalArgumentException("Unknown type " + parsedSpec.commandType());
    }
  }
}

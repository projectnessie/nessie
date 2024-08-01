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
package org.projectnessie.nessie.cli.commands;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Arrays.asList;
import static org.projectnessie.nessie.cli.cli.BaseNessieCli.STYLE_COMMAND_NAME;
import static org.projectnessie.nessie.cli.cli.BaseNessieCli.STYLE_HEADING;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Deque;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.jline.utils.AttributedString;
import org.jline.utils.AttributedStringBuilder;
import org.projectnessie.nessie.cli.cli.BaseNessieCli;
import org.projectnessie.nessie.cli.cmdspec.CommandSpec;
import org.projectnessie.nessie.cli.cmdspec.CommandType;
import org.projectnessie.nessie.cli.cmdspec.HelpCommandSpec;
import org.projectnessie.nessie.cli.grammar.Node;
import org.projectnessie.nessie.cli.grammar.Token;

public class HelpCommand extends NessieListingCommand<HelpCommandSpec> {
  public HelpCommand() {}

  @Override
  protected Stream<String> executeListing(BaseNessieCli cli, HelpCommandSpec spec) {
    List<Node> args = spec.getArguments();

    List<Node.NodeType> argTypes = args.stream().map(Node::getType).collect(Collectors.toList());

    if (argTypes.size() == 1) {
      if (argTypes.get(0) == Token.TokenType.LICENSE) {
        return licenseInfo();
      }
    }

    List<CommandType> matches =
        Arrays.stream(CommandType.values())
            .filter(
                t -> {
                  NessieCommand<CommandSpec> c = CommandsFactory.buildCommandInstance(t);
                  return args.isEmpty() || c.matchesNodeTypes().contains(argTypes);
                })
            .collect(Collectors.toList());

    Stream<String> output;

    if (matches.size() == 1) {
      output = exactMatch(cli, matches.get(0));
    } else {
      output = listAll(cli, matches);
    }

    return output;
  }

  private static Stream<String> licenseInfo() {
    Stream<String> lines = Stream.of("", "Nessie LICENSE", "==============", "");

    lines =
        Stream.concat(
            lines, Arrays.stream(fetchResource("META-INF/resources/LICENSE.txt").split("\n")));

    lines = Stream.concat(lines, Stream.of("", "", "Nessie NOTICE", "=============", ""));
    lines =
        Stream.concat(
            lines, Arrays.stream(fetchResource("META-INF/resources/NOTICE.txt").split("\n")));

    return lines;
  }

  private static Stream<String> listAll(BaseNessieCli cli, List<CommandType> matches) {
    Stream<String> output;

    output =
        matches.stream()
            .map(CommandsFactory::buildCommandInstance)
            .flatMap(
                c ->
                    Stream.of(
                        new AttributedString(c.name(), STYLE_COMMAND_NAME).toAnsi(cli.terminal()),
                        c.description(),
                        ""));

    Stream<String> heading = Stream.of("", "Nessie CLI - Help", "");
    output = Stream.concat(heading, output);
    return output;
  }

  private Stream<String> exactMatch(BaseNessieCli cli, CommandType commandType) {

    NessieCommand<CommandSpec> c = CommandsFactory.buildCommandInstance(commandType);

    Stream<String> heading =
        Stream.of(
            "",
            new AttributedStringBuilder()
                .append("Nessie CLI - ")
                .append(c.name(), STYLE_COMMAND_NAME)
                .append(" - ")
                .append(c.description())
                .toAnsi(cli.terminal()));

    String extensionPlain = ".plain.txt";
    String extension = cli.dumbTerminal() ? extensionPlain : ".ansi.txt";

    Stream<String> syntax =
        Stream.of(new AttributedString("Syntax:", STYLE_HEADING).toAnsi(cli.terminal()), "");

    for (String nonTerminalRef : nonTerminalRefs(commandType.statementName())) {
      // .trim() would remove trailing ESC chars as well, but we need those for proper ANSI output.
      String refSyntax =
          fetchSyntaxHelp(nonTerminalRef + extension).replaceAll("^\\s", "").replaceAll("\\s$", "");
      String refSyntaxPlain = fetchSyntaxHelp(nonTerminalRef + extensionPlain).trim();

      boolean isCommand = commandType.statementName().equals(nonTerminalRef);

      String help = fetchSyntaxHelp(nonTerminalRef + ".help.txt").trim();

      boolean showSyntax =
          !refSyntaxPlain.isEmpty()
              && !refSyntaxPlain.endsWith("Identifier")
              && !refSyntaxPlain.endsWith("PositiveInteger");
      boolean showHelp = !help.isEmpty();

      if (!isCommand && (showSyntax || showHelp)) {
        syntax =
            Stream.concat(
                syntax,
                Stream.of(
                    "\n"
                        + new AttributedString(nonTerminalRef + ":", STYLE_HEADING)
                            .toAnsi(cli.terminal())
                        + "\n"));
      }

      if (showSyntax) {
        syntax = Stream.concat(syntax, indent(refSyntax));
      }

      if (showHelp) {
        syntax =
            Stream.concat(
                syntax,
                isCommand
                    ? Stream.of(
                        "",
                        new AttributedString("Description:", STYLE_HEADING).toAnsi(cli.terminal()),
                        "",
                        help)
                    : Stream.of(help));
      }
    }

    syntax = Stream.concat(syntax, Stream.of(""));

    return Stream.concat(heading, syntax);
  }

  private static Stream<String> indent(String txt) {
    return Arrays.stream(txt.split("\n")).map(s -> "    " + s);
  }

  private static Set<String> nonTerminalRefs(String nonTerminalName) {
    Set<String> allRefs = new LinkedHashSet<>();

    Deque<String> deque = new ArrayDeque<>();
    deque.addLast(nonTerminalName);
    while (!deque.isEmpty()) {
      String name = deque.removeFirst();
      if (allRefs.add(name)) {
        deque.addAll(asList(fetchSyntaxHelp(nonTerminalName + ".refs").split("\n")));
      }
    }

    return allRefs;
  }

  private static String fetchSyntaxHelp(String resource) {
    return fetchResource("org/projectnessie/nessie/cli/syntax/" + resource);
  }

  private static String fetchResource(String resource) {
    URL url = HelpCommand.class.getClassLoader().getResource(resource);
    if (url == null) {
      return "";
    }
    try (InputStream in = url.openConnection().getInputStream()) {
      return new String(in.readAllBytes(), UTF_8);
    } catch (IOException e) {
      throw new RuntimeException("Failed to load resource " + url, e);
    }
  }

  public String name() {
    return Token.TokenType.HELP.name();
  }

  public String description() {
    return "Prints help information.";
  }

  @Override
  public List<List<Node.NodeType>> matchesNodeTypes() {
    return List.of(List.of(Token.TokenType.HELP));
  }
}

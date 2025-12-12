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
package org.projectnessie.nessie.cli.cli;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_CLIENT_NAME;
import static org.projectnessie.nessie.cli.commands.CommandsFactory.buildCommandInstance;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.net.URL;
import java.net.URLConnection;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.Callable;
import org.apache.iceberg.exceptions.ServiceFailureException;
import org.apache.iceberg.rest.RESTCatalog;
import org.jline.reader.EndOfFileException;
import org.jline.reader.LineReader;
import org.jline.reader.LineReaderBuilder;
import org.jline.reader.MaskingCallback;
import org.jline.reader.UserInterruptException;
import org.jline.reader.impl.DefaultParser;
import org.jline.terminal.Size;
import org.jline.terminal.Terminal;
import org.jline.terminal.TerminalBuilder;
import org.jline.utils.AttributedString;
import org.jline.utils.AttributedStringBuilder;
import org.jline.utils.AttributedStyle;
import org.projectnessie.api.NessieVersion;
import org.projectnessie.client.api.NessieApiV2;
import org.projectnessie.client.http.HttpClientException;
import org.projectnessie.client.rest.NessieServiceException;
import org.projectnessie.error.BaseNessieClientServerException;
import org.projectnessie.model.Branch;
import org.projectnessie.model.Detached;
import org.projectnessie.model.Reference;
import org.projectnessie.model.Tag;
import org.projectnessie.nessie.cli.cmdspec.CommandSpec;
import org.projectnessie.nessie.cli.cmdspec.ImmutableConnectCommandSpec;
import org.projectnessie.nessie.cli.commands.NessieCommand;
import org.projectnessie.nessie.cli.grammar.NessieCliParser;
import org.projectnessie.nessie.cli.grammar.Node;
import org.projectnessie.nessie.cli.grammar.ParseException;
import org.projectnessie.nessie.cli.grammar.Token;
import org.projectnessie.nessie.cli.grammar.ast.Script;
import org.projectnessie.nessie.cli.grammar.ast.SingleStatement;
import picocli.CommandLine;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(
    name = "nessie-cli.jar",
    description = {
      "",
      "The Nessie CLI",
      "See https://projectnessie.org/nessie-latest/cli/ for documentation.",
      ""
    },
    mixinStandardHelpOptions = true,
    versionProvider = NessieVersionProvider.class)
public class NessieCliImpl extends BaseNessieCli implements Callable<Integer> {

  public static final String OPTION_COMMAND = "--command";
  public static final String OPTION_SCRIPT = "--run-script";
  public static final String OPTION_QUIET = "--quiet";
  public static final String OPTION_NO_UP_TO_DATE_CHECK = "--no-up-to-date-check";
  public static final String OPTION_HISTORY = "--history";
  public static final String OPTION_HISTORY_FILE = "--history-file";
  public static final String OPTION_KEEP_RUNNING = "--keep-running";
  public static final String OPTION_CONTINUE_ON_ERROR = "--continue-on-error";
  public static final String OPTION_NON_ANSI = "--non-ansi";

  public static final String HISTORY_FILE_DEFAULT = "~/.nessie/nessie-cli.history";
  public static final AttributedStyle STYLE_ERROR_HIGHLIGHT = STYLE_ERROR.italic().bold();
  public static final AttributedStyle STYLE_ERROR_DELIGHT = STYLE_ERROR.italic().faint();
  public static final AttributedStyle STYLE_ERROR_EMPHASIZE1 = STYLE_ERROR.italic().underline();
  public static final AttributedStyle STYLE_ERROR_EMPHASIZE2 =
      STYLE_ERROR.bold().italic().underline();

  private String prompt;
  private String rightPrompt;

  @ArgGroup(
      exclusive = false,
      heading =
          "\n"
              + "Statements to execute before or without running the REPL\n"
              + "========================================================\n"
              + "\n")
  private CommandsToRun commandsToRun;

  @Option(
      names = OPTION_NON_ANSI,
      description = {
        "Allows disabling the (default) ANSI mode. Disabling ANSI support can be useful in non-interactive scripts."
      },
      defaultValue = "false")
  private boolean dumbTerminal;

  @Option(
      names = {"-q", OPTION_QUIET},
      description = {"Quiet option - omit the welcome and exit output."})
  private boolean quiet;

  @Option(
      names = {OPTION_NO_UP_TO_DATE_CHECK},
      description = {
        "Optionally disable the up-to-date check.",
        "Only effective if " + OPTION_QUIET + " is not specified."
      })
  private boolean noUpToDateCheck;

  @SuppressWarnings("FieldCanBeLocal")
  @Option(
      names = {"-H", OPTION_HISTORY},
      description = {
        "Allows disabling the command history file in the REPL.",
        "Default is to save the command history."
      },
      defaultValue = "true",
      negatable = true)
  private boolean history = true;

  @Option(
      names = {"-j", OPTION_HISTORY_FILE},
      description = {"Specify an alternative history file for the REPL."},
      defaultValue = HISTORY_FILE_DEFAULT,
      showDefaultValue = CommandLine.Help.Visibility.ALWAYS)
  private Path historyFile = Paths.get(HISTORY_FILE_DEFAULT);

  @ArgGroup(exclusive = false, heading = "\n" + "Connect options\n" + "===============\n" + "\n")
  private ConnectOptions connectOptions;

  public NessieCliImpl() {
    updatePrompt();
  }

  @Override
  public void setCurrentReference(Reference currentReference) {
    super.setCurrentReference(currentReference);
    updatePrompt();
  }

  @Override
  public void connected(NessieApiV2 nessieApi, RESTCatalog icebergClient) {
    super.connected(nessieApi, icebergClient);
    updatePrompt();
  }

  @Override
  public Integer call() throws Exception {
    Terminal terminal =
        TerminalBuilder.builder()
            .jansi(!dumbTerminal)
            .dumb(dumbTerminal)
            .provider(dumbTerminal ? TerminalBuilder.PROP_PROVIDER_DUMB : null)
            .build();

    setTerminal(terminal);

    // hard coded terminal size when redirecting (redirect detection doesn't work properly in jline
    // though :( )
    if (terminal.getWidth() == 0 || terminal.getHeight() == 0) {
      terminal.setSize(new Size(120, 40));
    }

    @SuppressWarnings("resource")
    PrintWriter writer = writer();
    try {
      if (!quiet) {
        writer.print(readResource(dumbTerminal ? "banner-plain.txt" : "banner.txt"));
        writer.printf("v%s%n%n", NessieVersion.NESSIE_VERSION);
        writer.print(readResource("welcome.txt"));

        if (history) {
          writer.println(
              String.format(
                  "History file in %s will record all statements.\n"
                      + "Tip: lines that start with a space (' ') are not recorded in the history.\n\n",
                  historyFile));
        }

        checkVersion();

        RuntimeMXBean rb = ManagementFactory.getRuntimeMXBean();
        long uptime = rb.getUptime();
        writer.printf("Nessie CLI startup took %d ms.%n%n", uptime);
      }

      if (connectOptions != null) {
        if (!connectTo(connectOptions)) {
          return 1;
        }
      }

      CommandsToRun commandsToRun = this.commandsToRun;

      if (commandsToRun != null) {
        runCommands(commandsToRun);
      }

      if (commandsToRun == null || commandsToRun.keepRunning) {
        runRepl();
      }
    } finally {
      writer.flush();
    }

    return getExitWithCode();
  }

  private void checkVersion() {
    if (noUpToDateCheck) {
      return;
    }

    Terminal terminal = terminal();
    @SuppressWarnings("resource")
    PrintWriter writer = writer();

    String version = NessieVersion.NESSIE_VERSION;
    if (version.endsWith("-SNAPSHOT")) {
      writer.println();
      writer.println(
          new AttributedString("You are running a SNAPSHOT version of Nessie CLI.", STYLE_YELLOW)
              .toAnsi(terminal));
    } else {
      try {
        String latestNessieReleaseUrl =
            "https://api.github.com/repos/projectnessie/nessie/releases/latest";
        URL url = new URL(latestNessieReleaseUrl);
        URLConnection conn = requireNonNull(url, latestNessieReleaseUrl).openConnection();
        ObjectNode latestRelease;
        try (var input = conn.getInputStream()) {
          latestRelease = new ObjectMapper().readValue(input, ObjectNode.class);
        }

        String latestReleaseVersion = latestRelease.get("tag_name").asText().replace("nessie-", "");
        if (!version.equals(latestReleaseVersion)) {
          writer.println(
              new AttributedString(
                      "You are running version "
                          + version
                          + ", which is not the latest release version "
                          + latestReleaseVersion
                          + ".",
                      STYLE_YELLOW)
                  .toAnsi(terminal));
          writer.println(
              "Please download the latest version of the Nessie CLI from "
                  + latestRelease.get("html_url").asText());
        } else {
          writer.println("You are running the latest release of Nessie CLI");
        }
      } catch (IOException e) {
        writer.println(
            "Cannot check for the latest Nessie release version: "
                + new AttributedString(e.toString(), STYLE_ERROR).toAnsi(terminal));
      }
    }

    writer.println();
  }

  boolean connectTo(ConnectOptions connectOptions) {
    if (connectOptions.uri == null) {
      @SuppressWarnings("resource")
      PrintWriter writer = writer();
      writer.println(
          new AttributedString(
                  "Command line option --uri is mandatory when using any of the 'Connect options'",
                  STYLE_ERROR.bold())
              .toAnsi(terminal()));
      return false;
    }
    ImmutableConnectCommandSpec.Builder specBuilder =
        ImmutableConnectCommandSpec.builder()
            .uri(connectOptions.uri.toString())
            .initialReference(connectOptions.initialReference);
    if (connectOptions.clientName != null) {
      specBuilder.putParameter(CONF_NESSIE_CLIENT_NAME, connectOptions.clientName);
    }
    if (connectOptions.clientOptions != null) {
      connectOptions.clientOptions.forEach(specBuilder::putParameter);
    }
    return executeCommand(null, specBuilder.build());
  }

  void runCommands(CommandsToRun commandsToRun) {
    CommandsToRun.CommandsSource source = commandsToRun.commandsSource;

    if (source.scriptFile != null) {
      if (!runScript(commandsToRun)) {
        return;
      }
    }

    for (String command : source.commands) {
      if (!quiet) {
        @SuppressWarnings("resource")
        PrintWriter writer = writer();
        writer.println(
            new AttributedStringBuilder()
                .append("Nessie> ", STYLE_FAINT)
                .append(command)
                .toAnsi(terminal()));
      }

      if (!parseAndExecuteSingleStatement(command)) {
        if (!commandsToRun.continueOnError) {
          setExitWithCode(1);
          return;
        }
      }
    }
  }

  boolean runScript(CommandsToRun commandsToRun) {
    CommandsToRun.CommandsSource source = commandsToRun.commandsSource;

    String scriptSource;
    try {
      if ("-".equals(source.scriptFile)) {
        StringWriter sw = new StringWriter();
        @SuppressWarnings("resource")
        Terminal terminal = terminal();
        terminal.reader().transferTo(sw);
        scriptSource = sw.toString();
      } else {
        scriptSource = Files.readString(Paths.get(source.scriptFile));
      }
    } catch (IOException e) {
      handleException(e, null, null);
      return false;
    }

    if (scriptSource.isBlank()) {
      return true;
    }

    return parseAndExecuteScript(scriptSource, commandsToRun.continueOnError, true);
  }

  boolean parseAndExecuteScript(
      String scriptSource, boolean continueOnError, boolean echoStatement) {
    NessieCliParser parser = newParserForSource(scriptSource);

    try {
      parser.Script();
      Node root = parser.rootNode();
      Script script = (Script) root;
      for (CommandSpec commandSpec : script.getCommandSpecs()) {

        if (echoStatement) {
          Node node = commandSpec.sourceNode();
          if (node != null) {
            String command = scriptSource.substring(node.getBeginOffset(), node.getEndOffset());
            @SuppressWarnings("resource")
            PrintWriter writer = writer();
            writer.println(
                new AttributedStringBuilder()
                    .append("Nessie> ", STYLE_FAINT)
                    .append(command)
                    .toAnsi(terminal()));
          }
        }

        if (!executeCommand(scriptSource, commandSpec) && !continueOnError) {
          return false;
        }
      }
      return true;
    } catch (ParseException e) {
      handleParseException(scriptSource, e);
    }

    return true;
  }

  boolean parseAndExecuteSingleStatement(String line) {
    NessieCliParser parser = newParserForSource(line);
    try {
      parser.SingleStatement();
      Node root = parser.rootNode();
      SingleStatement singleStatement = (SingleStatement) root;
      CommandSpec commandSpec = singleStatement.getCommandSpec();
      return executeCommand(line, commandSpec);
    } catch (ParseException e) {
      handleParseException(line, e);
    }

    return false;
  }

  boolean executeCommand(String source, CommandSpec commandSpec) {
    @SuppressWarnings("resource")
    PrintWriter writer = writer();

    writer.flush();

    try {
      NessieCommand<CommandSpec> nessieCommand = buildCommandInstance(commandSpec.commandType());
      nessieCommand.execute(this, commandSpec);

      return true;
    } catch (CliCommandFailedException e) {
      return false;
    } catch (BaseNessieClientServerException | NotConnectedException e) {
      AttributedStringBuilder errMsg = new AttributedStringBuilder();
      errMsg.append(e.getMessage(), STYLE_ERROR);
      writer.println(errMsg.toAnsi(terminal()));
    } catch (RuntimeException e) {
      Throwable c = e.getCause();
      if (c instanceof BaseNessieClientServerException) {
        AttributedStringBuilder errMsg = new AttributedStringBuilder();
        errMsg.append(e.getMessage(), STYLE_ERROR);
        writer.println(errMsg.toAnsi(terminal()));
      } else {
        handleException(e, commandSpec, source);
      }
    } catch (Exception e) {
      handleException(e, commandSpec, source);
    } finally {
      writer.flush();
    }

    return false;
  }

  void handleException(Exception e, CommandSpec commandSpec, String source) {
    AttributedStringBuilder errMsg = new AttributedStringBuilder();

    if (commandSpec != null) {
      Node node = commandSpec.sourceNode();

      if (node != null) {
        errMsg
            .append("Encountered an error executing the following statement", STYLE_ERROR)
            .append("\n    ");
        if (source != null) {
          errMsg.append(
              source.substring(node.getBeginOffset(), node.getEndOffset()), STYLE_ERROR.italic());
        }
      }
    }

    errMsg.append("\n\n");
    if (!(e instanceof HttpClientException)
        && !(e instanceof ServiceFailureException)
        && !(e instanceof BaseNessieClientServerException)
        && !(e instanceof NessieServiceException)
        && !(e instanceof IllegalArgumentException)) {
      StringWriter sw = new StringWriter();
      e.printStackTrace(new PrintWriter(sw));
      String stackTrace = sw.toString();
      stackTrace = stackTrace.substring(stackTrace.indexOf('\n') + 1);
      errMsg
          .append(e.toString(), STYLE_ERROR_HIGHLIGHT)
          .append('\n')
          .append(stackTrace, STYLE_ITALIC_BOLD);
    } else {
      errMsg
          .append(e.getClass().getSimpleName() + ": ", STYLE_ERROR_DELIGHT)
          .append(e.getMessage(), STYLE_ERROR_HIGHLIGHT);
      String last = "";
      for (Throwable cause = e.getCause(); cause != null; cause = cause.getCause()) {
        String m = cause.toString();
        if (m.equals(last)) {
          continue;
        }
        errMsg.append("\n    caused by: ").append(m, AttributedStyle.BOLD);
        last = m;
      }
    }
    errMsg.append("\n\n");

    @SuppressWarnings("resource")
    PrintWriter writer = writer();
    writer.println(errMsg.toAnsi(terminal()));
  }

  void handleParseException(String input, ParseException e) {
    Node t = e.getToken();
    AttributedStringBuilder errMsg = new AttributedStringBuilder();

    errMsg
        .append(
            format(
                "Encountered an error parsing the statement around line %d, column %d .. line %d column %d",
                t.getBeginLine(), t.getBeginColumn(), t.getEndLine(), t.getEndColumn()),
            STYLE_ERROR)
        .append("\n\nFound: ")
        .append(input.substring(t.getBeginOffset(), t.getEndOffset()), STYLE_ERROR_EMPHASIZE1)
        .append("\nExpected one of the following: ");
    boolean first = true;
    for (Token.TokenType type : e.getExpectedTokenTypes()) {
      if (first) {
        first = false;
      } else {
        errMsg.append(" , ");
      }
      errMsg.append(type.name(), STYLE_YELLOW.italic());
    }
    errMsg
        .append("\n\n")
        .append(input.substring(0, t.getBeginOffset()))
        .append(input.substring(t.getBeginOffset(), t.getEndOffset()), STYLE_ERROR_EMPHASIZE2)
        .append(input.substring(t.getEndOffset()))
        .append("\n\n");

    @SuppressWarnings("resource")
    PrintWriter writer = writer();
    writer.println(errMsg.toAnsi(terminal()));
  }

  private void runRepl() {

    LineReaderBuilder readerBuilder =
        LineReaderBuilder.builder()
            .terminal(terminal())
            //
            .parser(
                new DefaultParser()
                    .blockCommentDelims(new DefaultParser.BlockCommentDelims("/*", "*/"))
                    .lineCommentDelims(new String[] {"--"}))
            //
            .completer(new NessieCliCompleter(this))
            //
            .highlighter(new NessieCliHighlighter())
            //
            .variable(LineReader.INDENTATION, 2)
            .variable(LineReader.LIST_MAX, 100)
            //
            // Nessie CLI syntax is case-insensitive
            .option(LineReader.Option.CASE_INSENSITIVE_SEARCH, true)
            .option(LineReader.Option.CASE_INSENSITIVE, true)
            //
            .option(
                LineReader.Option.USE_FORWARD_SLASH,
                true) // use forward slash in directory separator
            .option(LineReader.Option.DISABLE_EVENT_EXPANSION, true);

    if (history) {
      if (historyFile.getNameCount() > 0) {
        Path f = historyFile;
        String first = f.getName(0).toString();
        if ("$HOME".equals(first) || "%HOME%".equals(first) || "~".equals(first)) {
          Path userDir = Paths.get(System.getProperty("user.home"));
          f = userDir.resolve(f.subpath(1, f.getNameCount()));
        }

        try {
          Files.createDirectories(f.getParent());
        } catch (IOException e) {
          // ignore for now
        }

        readerBuilder.variable(LineReader.HISTORY_FILE, f);
      }
    } else {
      readerBuilder.variable(LineReader.DISABLE_HISTORY, true);
    }

    LineReader reader = readerBuilder.build();
    while (getExitWithCode() == null) {
      String line;
      try {
        line = reader.readLine(prompt, rightPrompt, (MaskingCallback) null, null);
      } catch (UserInterruptException e) {
        // ignore
        continue;
      } catch (EndOfFileException e) {
        break;
      }

      if (line.trim().isBlank()) {
        continue;
      }

      parseAndExecuteScript(line, false, false);
    }

    if (!quiet) {
      @SuppressWarnings("resource")
      PrintWriter writer = writer();
      writer.println("\nBye\n");
    }
  }

  private void updatePrompt() {
    AttributedStringBuilder prompt = new AttributedStringBuilder();
    AttributedStringBuilder rightPrompt = new AttributedStringBuilder();

    if (nessieApi().isEmpty()) {
      prompt.append("Not connected - use 'CONNECT TO' statement\n", STYLE_YELLOW.bold());
      prompt.append("Nessie", STYLE_YELLOW.bold());
    } else {
      Reference currentReference = getCurrentReference();
      if (currentReference instanceof Branch) {
        prompt.append(currentReference.getName(), STYLE_GREEN.bold());
      } else if (currentReference instanceof Tag) {
        prompt.append(currentReference.getName(), STYLE_BLUE.bold());
      } else if (currentReference instanceof Detached) {
        if (currentReference.getHash() != null) {
          prompt.append('@');
          prompt.append(currentReference.getHash(), STYLE_FAINT.italic());
        }
      }
    }
    prompt.append("> ");

    this.prompt = prompt.toAnsi();
    this.rightPrompt = rightPrompt.toAnsi(terminal());
  }
}

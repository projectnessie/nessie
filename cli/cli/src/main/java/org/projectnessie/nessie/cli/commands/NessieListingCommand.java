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

import jakarta.annotation.Nonnull;
import java.io.InputStream;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Stream;
import org.jline.builtins.Less;
import org.jline.builtins.Options;
import org.jline.builtins.Source;
import org.jline.terminal.Terminal;
import org.projectnessie.nessie.cli.cli.BaseNessieCli;
import org.projectnessie.nessie.cli.cmdspec.CommandSpec;

public abstract class NessieListingCommand<SPEC extends CommandSpec> extends NessieCommand<SPEC> {

  public void execute(@Nonnull BaseNessieCli cli, SPEC spec) throws Exception {
    Terminal terminal = cli.terminal();
    PrintStream ps = new PrintStream(terminal.output());

    // Capture Ctrl-C - no-op within 'less', because otherwise ctrl-C kills the whole process :(
    Terminal.SignalHandler prevHandler = terminal.handle(Terminal.Signal.INT, sig -> {});
    try {

      Stream<String> listing = executeListing(cli, spec);

      if (cli.dumbTerminal()) {
        @SuppressWarnings("resource")
        PrintWriter writer = cli.writer();
        listing.forEach(writer::println);
      } else {
        InputStream in = new LinesInputStream(listing);

        Less less =
            new Less(
                terminal,
                Paths.get("."),
                Options.compile(Less.usage()).parse(List.of("--quit-if-one-screen")));

        less.run(new Source.InputStreamSource(in, false, spec.commandType().statementName()));
      }

    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    } finally {
      if (prevHandler != null) {
        terminal.handle(Terminal.Signal.INT, prevHandler);
      }

      ps.flush();
    }
  }

  protected abstract Stream<String> executeListing(BaseNessieCli nessieCli, SPEC commandSpec)
      throws Exception;
}

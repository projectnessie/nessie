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

import static java.lang.String.format;

import jakarta.annotation.Nonnull;
import java.io.PrintWriter;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import java.util.function.Predicate;
import org.jline.terminal.Terminal;
import org.jline.utils.AttributedString;
import org.jline.utils.AttributedStyle;
import org.projectnessie.client.NessieClientBuilder;
import org.projectnessie.client.api.NessieApiV2;
import org.projectnessie.client.config.NessieClientConfigSources;
import org.projectnessie.model.NessieConfiguration;
import org.projectnessie.model.Reference;
import org.projectnessie.nessie.cli.cli.BaseNessieCli;
import org.projectnessie.nessie.cli.cmdspec.ConnectCommandSpec;
import org.projectnessie.nessie.cli.grammar.Node;
import org.projectnessie.nessie.cli.grammar.Token;

public class ConnectCommand extends NessieCommand<ConnectCommandSpec> {
  public ConnectCommand() {}

  @Override
  public void execute(@Nonnull BaseNessieCli cli, ConnectCommandSpec spec) throws Exception {

    PrintWriter writer = cli.writer();

    writer.println(
        new AttributedString(
            format("Connecting to %s ...", spec.getUri()), AttributedStyle.DEFAULT.faint()));
    writer.flush();

    NessieApiV2 api = null;
    NessieConfiguration config;

    CompletableFuture<?> cancel = new CompletableFuture<>();
    Terminal terminal = cli.terminal();
    Terminal.SignalHandler sigIntHandler =
        terminal.handle(Terminal.Signal.INT, sig -> cancel.completeAsync(() -> null));
    try {
      api =
          NessieClientBuilder.createClientBuilderFromSystemSettings(
                  NessieClientConfigSources.mapConfigSource(spec.getParameters()))
              .withUri(spec.getUri())
              .withCancellationFuture(cancel)
              .build(NessieApiV2.class);

      config = api.getConfig();
    } catch (Exception e) {
      if (api != null) {
        api.close();
      }

      if (hasCauseMatching(
          e,
          t ->
              t instanceof CancellationException
                  || t instanceof InterruptedException
                  || t instanceof TimeoutException)) {
        writer.println(
            new AttributedString(
                    "Connection request aborted or timed out.",
                    AttributedStyle.DEFAULT.foreground(128, 128, 0))
                .toAnsi(cli.terminal()));
        writer.println();
        writer.flush();

        return;
      }
      throw e;
    } finally {
      if (sigIntHandler != null) {
        terminal.handle(Terminal.Signal.INT, sigIntHandler);
      }
    }

    writer.printf(
        "Successfully connected to %s - Nessie API version %d, spec version %s%n",
        spec.getUri(), config.getActualApiVersion(), config.getSpecVersion());

    cli.connected(api);

    Reference ref;
    if (spec.getInitialReference() != null) {
      ref = api.getReference().refName(spec.getInitialReference()).get();
    } else {
      ref = api.getDefaultBranch();
    }

    cli.setCurrentReference(ref);
  }

  static boolean hasCauseMatching(Throwable t, Predicate<Throwable> test) {
    for (; t != null; t = t.getCause()) {
      if (test.test(t)) {
        return true;
      }
    }
    return false;
  }

  public String name() {
    return Token.TokenType.CONNECT.name();
  }

  public String description() {
    return "Connect to a Nessie repository.";
  }

  @Override
  public List<List<Node.NodeType>> matchesNodeTypes() {
    return List.of(List.of(Token.TokenType.CONNECT));
  }
}

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

import static org.projectnessie.nessie.cli.cli.BaseNessieCli.STYLE_FAINT;

import jakarta.annotation.Nonnull;
import java.util.List;
import org.jline.terminal.Terminal;
import org.jline.utils.AttributedStringBuilder;
import org.projectnessie.client.api.NessieApiV2;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Reference;
import org.projectnessie.nessie.cli.cli.BaseNessieCli;
import org.projectnessie.nessie.cli.cmdspec.RefCommandSpec;
import org.projectnessie.nessie.cli.cmdspec.ShowReferenceCommandSpec;
import org.projectnessie.nessie.cli.grammar.Node;
import org.projectnessie.nessie.cli.grammar.Token;

public class ShowReferenceCommand extends NessieCommand<ShowReferenceCommandSpec> {
  public ShowReferenceCommand() {}

  @Override
  public void execute(@Nonnull BaseNessieCli cli, ShowReferenceCommandSpec spec)
      throws NessieNotFoundException {
    @SuppressWarnings("resource")
    NessieApiV2 api = cli.mandatoryNessieApi();

    String refName = ((RefCommandSpec) spec).getRef();
    if (refName == null) {
      refName = cli.getCurrentReference().getName();
    }

    Reference ref = api.getReference().refName(refName).get();

    Terminal terminal = cli.terminal();
    terminal
        .writer()
        .println(
            new AttributedStringBuilder()
                .append("Reference type: ")
                .append(ref.getType().name(), STYLE_FAINT)
                .toAnsi(terminal));
    terminal
        .writer()
        .println(
            new AttributedStringBuilder()
                .append("          Name: ")
                .append(ref.getName())
                .toAnsi(terminal));
    terminal
        .writer()
        .println(
            new AttributedStringBuilder()
                .append("      Tip/HEAD: ")
                .append(ref.getHash(), STYLE_FAINT)
                .toAnsi(terminal));
  }

  public String name() {
    return Token.TokenType.SHOW + " " + Token.TokenType.REFERENCE;
  }

  public String description() {
    return "Information about a reference.";
  }

  @Override
  public List<List<Node.NodeType>> matchesNodeTypes() {
    return List.of(
        List.of(Token.TokenType.SHOW), List.of(Token.TokenType.SHOW, Token.TokenType.REFERENCE));
  }
}

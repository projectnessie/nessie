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
import java.io.PrintWriter;
import java.util.List;
import org.jline.utils.AttributedStringBuilder;
import org.jline.utils.AttributedStyle;
import org.projectnessie.client.api.NessieApiV2;
import org.projectnessie.model.Reference;
import org.projectnessie.nessie.cli.cli.BaseNessieCli;
import org.projectnessie.nessie.cli.cmdspec.UseReferenceCommandSpec;
import org.projectnessie.nessie.cli.grammar.Node;
import org.projectnessie.nessie.cli.grammar.Token;

public class UseReferenceCommand extends NessieCommand<UseReferenceCommandSpec> {
  public UseReferenceCommand() {}

  @Override
  public void execute(@Nonnull BaseNessieCli cli, UseReferenceCommandSpec spec) throws Exception {
    @SuppressWarnings("resource")
    NessieApiV2 api = cli.mandatoryNessieApi();

    // TODO handle `commandSpec.getRefType()`

    Reference ref = api.getReference().refName(spec.getRef()).get();

    cli.setCurrentReference(ref);

    @SuppressWarnings("resource")
    PrintWriter writer = cli.writer();
    writer.println(
        new AttributedStringBuilder()
            .append("Using ")
            .append(ref.getName(), AttributedStyle.BOLD)
            .append(" at ")
            .append(ref.getHash(), AttributedStyle.DEFAULT.faint()));
  }

  public String name() {
    return Token.TokenType.USE + " " + Token.TokenType.REFERENCE;
  }

  public String description() {
    return "Make the given reference the current reference.";
  }

  @Override
  public List<List<Node.NodeType>> matchesNodeTypes() {
    return List.of(
        List.of(Token.TokenType.USE), List.of(Token.TokenType.USE, Token.TokenType.REFERENCE));
  }
}

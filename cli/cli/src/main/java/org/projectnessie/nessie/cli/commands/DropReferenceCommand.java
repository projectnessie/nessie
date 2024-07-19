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
import static org.projectnessie.nessie.cli.cli.BaseNessieCli.STYLE_BOLD;
import static org.projectnessie.nessie.cli.cli.BaseNessieCli.STYLE_FAINT;

import jakarta.annotation.Nonnull;
import java.io.PrintWriter;
import java.util.List;
import org.jline.utils.AttributedStringBuilder;
import org.projectnessie.client.api.NessieApiV2;
import org.projectnessie.error.NessieReferenceNotFoundException;
import org.projectnessie.model.Reference;
import org.projectnessie.nessie.cli.cli.BaseNessieCli;
import org.projectnessie.nessie.cli.cmdspec.DropReferenceCommandSpec;
import org.projectnessie.nessie.cli.grammar.Node;
import org.projectnessie.nessie.cli.grammar.Token;

public class DropReferenceCommand extends NessieCommand<DropReferenceCommandSpec> {
  public DropReferenceCommand() {}

  @Override
  public void execute(@Nonnull BaseNessieCli cli, DropReferenceCommandSpec spec) throws Exception {
    @SuppressWarnings("resource")
    NessieApiV2 api = cli.mandatoryNessieApi();

    Reference reference = api.getReference().refName(spec.getRef()).get();

    if (Reference.ReferenceType.valueOf(spec.getRefType()) != reference.getType()) {
      throw new IllegalArgumentException(
          format(
              "'%s' is not a %s but a %s.", spec.getRef(), spec.getRefType(), reference.getType()));
    }

    if (spec.getRef().equals(cli.getCurrentReference().getName())) {
      throw new IllegalArgumentException("Must not delete the current reference.");
    }

    @SuppressWarnings("resource")
    PrintWriter writer = cli.writer();

    Reference deleted;
    try {
      deleted = api.deleteReference().reference(reference).getAndDelete();
    } catch (NessieReferenceNotFoundException e) {
      if (spec.isConditional()) {
        writer.println(
            new AttributedStringBuilder()
                .append("Did not delete reference ")
                .append(spec.getRef())
                .append(", because such a reference does not exist."));
        return;
      }
      throw e;
    }

    writer.println(
        new AttributedStringBuilder()
            .append("Deleted ")
            .append(deleted.getType().name())
            .append(' ')
            .append(deleted.getName(), STYLE_BOLD)
            .append(" at ")
            .append(deleted.getHash(), STYLE_FAINT));
  }

  public String name() {
    return Token.TokenType.DROP + " " + Token.TokenType.BRANCH + "/" + Token.TokenType.TAG;
  }

  public String description() {
    return "Delete a branch or tag.";
  }

  @Override
  public List<List<Node.NodeType>> matchesNodeTypes() {
    return List.of(
        List.of(Token.TokenType.DROP),
        List.of(Token.TokenType.DROP, Token.TokenType.BRANCH),
        List.of(Token.TokenType.DROP, Token.TokenType.TAG));
  }
}

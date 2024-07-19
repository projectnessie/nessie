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

import static org.projectnessie.nessie.cli.cli.BaseNessieCli.STYLE_BOLD;
import static org.projectnessie.nessie.cli.cli.BaseNessieCli.STYLE_FAINT;

import jakarta.annotation.Nonnull;
import java.io.PrintWriter;
import java.util.List;
import org.jline.utils.AttributedStringBuilder;
import org.projectnessie.client.api.NessieApiV2;
import org.projectnessie.error.NessieReferenceAlreadyExistsException;
import org.projectnessie.model.Branch;
import org.projectnessie.model.Reference;
import org.projectnessie.model.Tag;
import org.projectnessie.nessie.cli.cli.BaseNessieCli;
import org.projectnessie.nessie.cli.cmdspec.CreateReferenceCommandSpec;
import org.projectnessie.nessie.cli.grammar.Node;
import org.projectnessie.nessie.cli.grammar.Token;

public class CreateReferenceCommand extends NessieCommand<CreateReferenceCommandSpec> {
  public CreateReferenceCommand() {}

  @Override
  public void execute(@Nonnull BaseNessieCli cli, CreateReferenceCommandSpec spec)
      throws Exception {
    @SuppressWarnings("resource")
    NessieApiV2 api = cli.mandatoryNessieApi();

    Reference currentRef = api.getReference().refName(cli.getCurrentReference().getName()).get();

    String hash = hashOrTimestamp(spec);
    if (hash == null) {
      hash = currentRef.getHash();
    }

    Reference.ReferenceType referenceType = Reference.ReferenceType.valueOf(spec.getRefType());
    Reference reference;
    switch (referenceType) {
      case BRANCH:
        reference = Branch.of(spec.getRef(), hash);
        break;
      case TAG:
        reference = Tag.of(spec.getRef(), hash);
        break;
      default:
        throw new IllegalArgumentException("Unknown reference type: " + spec.getRefType());
    }

    Reference created;
    try {
      created =
          api.createReference().reference(reference).sourceRefName(currentRef.getName()).create();
    } catch (NessieReferenceAlreadyExistsException e) {
      if (spec.isConditional()) {
        @SuppressWarnings("resource")
        PrintWriter writer = cli.writer();
        writer.println(
            new AttributedStringBuilder()
                .append("Did not create ")
                .append(referenceType.name())
                .append(' ')
                .append(spec.getRef())
                .append(", because a reference with that name already exists."));
        return;
      }
      throw e;
    }

    @SuppressWarnings("resource")
    PrintWriter writer = cli.writer();
    writer.println(
        new AttributedStringBuilder()
            .append("Created ")
            .append(created.getType().name())
            .append(' ')
            .append(created.getName(), STYLE_BOLD)
            .append(" at ")
            .append(created.getHash(), STYLE_FAINT));
  }

  public String name() {
    return Token.TokenType.CREATE + " " + Token.TokenType.BRANCH + "/" + Token.TokenType.TAG;
  }

  public String description() {
    return "Create a branch or tag.";
  }

  @Override
  public List<List<Node.NodeType>> matchesNodeTypes() {
    return List.of(
        List.of(Token.TokenType.CREATE),
        List.of(Token.TokenType.CREATE, Token.TokenType.BRANCH),
        List.of(Token.TokenType.CREATE, Token.TokenType.TAG));
  }
}

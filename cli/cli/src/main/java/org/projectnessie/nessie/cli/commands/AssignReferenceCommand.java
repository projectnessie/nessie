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
import org.projectnessie.model.Branch;
import org.projectnessie.model.Reference;
import org.projectnessie.model.Tag;
import org.projectnessie.nessie.cli.cli.BaseNessieCli;
import org.projectnessie.nessie.cli.cmdspec.AssignReferenceCommandSpec;
import org.projectnessie.nessie.cli.grammar.Node;
import org.projectnessie.nessie.cli.grammar.Token;

public class AssignReferenceCommand extends NessieCommand<AssignReferenceCommandSpec> {
  public AssignReferenceCommand() {}

  @Override
  public void execute(@Nonnull BaseNessieCli cli, AssignReferenceCommandSpec spec)
      throws Exception {
    @SuppressWarnings("resource")
    NessieApiV2 api = cli.mandatoryNessieApi();

    Reference ref = api.getReference().refName(spec.getRef()).get();

    Reference toRef = api.getReference().refName(spec.getTo()).get();
    String hash = hashOrTimestamp(spec);
    if (hash == null) {
      hash = toRef.getHash();
    }

    Reference.ReferenceType referenceType = Reference.ReferenceType.valueOf(spec.getRefType());

    switch (referenceType) {
      case BRANCH:
        toRef = Branch.of(toRef.getName(), hash);
        break;
      case TAG:
        toRef = Tag.of(toRef.getName(), hash);
        break;
      default:
        throw new IllegalArgumentException("Unknown reference type: " + spec.getRefType());
    }

    Reference assigned = api.assignReference().reference(ref).assignTo(toRef).assignAndGet();

    if (ref.getName().equals(cli.getCurrentReference().getName())) {
      cli.setCurrentReference(assigned);
    }

    @SuppressWarnings("resource")
    PrintWriter writer = cli.writer();
    writer.println(
        new AttributedStringBuilder()
            .append("Assigned ")
            .append(assigned.getType().name())
            .append(' ')
            .append(assigned.getName(), STYLE_BOLD)
            .append(" from ")
            .append(ref.getHash(), STYLE_FAINT)
            .append(" to ")
            .append(assigned.getHash(), STYLE_FAINT));
  }

  public String name() {
    return Token.TokenType.ASSIGN + " " + Token.TokenType.BRANCH + "/" + Token.TokenType.TAG;
  }

  public String description() {
    return "Assign the tip of a branch or tag to a commit ID.";
  }

  @Override
  public List<List<Node.NodeType>> matchesNodeTypes() {
    return List.of(
        List.of(Token.TokenType.ASSIGN),
        List.of(Token.TokenType.ASSIGN, Token.TokenType.BRANCH),
        List.of(Token.TokenType.ASSIGN, Token.TokenType.BRANCH));
  }
}

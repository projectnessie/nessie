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
import java.util.Locale;
import org.jline.utils.AttributedStringBuilder;
import org.jline.utils.AttributedStyle;
import org.projectnessie.client.api.CommitMultipleOperationsBuilder;
import org.projectnessie.model.Branch;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.CommitResponse;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.DeltaLakeTable;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.IcebergView;
import org.projectnessie.model.Namespace;
import org.projectnessie.model.Operation;
import org.projectnessie.nessie.cli.cli.BaseNessieCli;
import org.projectnessie.nessie.cli.cmdspec.DropContentCommandSpec;
import org.projectnessie.nessie.cli.grammar.Node;
import org.projectnessie.nessie.cli.grammar.Token;

public class DropContentCommand extends NessieCommittingCommand<DropContentCommandSpec> {
  public DropContentCommand() {}

  @Override
  protected CommitResponse executeCommitting(
      @Nonnull BaseNessieCli cli,
      DropContentCommandSpec spec,
      Branch branch,
      CommitMultipleOperationsBuilder commit)
      throws Exception {
    ContentKey contentKey = ContentKey.fromPathString(spec.getContentKey());

    @SuppressWarnings("resource")
    Content content =
        applyReference(cli, spec, cli.mandatoryNessieApi().getContent())
            .getSingle(contentKey)
            .getContent();

    switch (spec.getContentKind()) {
      case "TABLE":
        if (!(content instanceof IcebergTable) && !(content instanceof DeltaLakeTable)) {
          throw new IllegalArgumentException(
              "Key " + contentKey + " is not a table, but a " + content.getType());
        }
        break;
      case "VIEW":
        if (!(content instanceof IcebergView)) {
          throw new IllegalArgumentException(
              "Key " + contentKey + " is not a view, but a " + content.getType());
        }
        break;
      case "NAMESPACE":
        if (!(content instanceof Namespace)) {
          throw new IllegalArgumentException(
              "Key " + contentKey + " is not a namespace, but a " + content.getType());
        }
        break;
      default:
        throw new UnsupportedOperationException(
            "Content kind not supported: " + spec.getContentKind());
    }

    String kind = spec.getContentKind().toLowerCase(Locale.ROOT);
    CommitResponse committed =
        commit
            .commitMeta(CommitMeta.fromMessage("Drop " + kind + " " + contentKey))
            .operation(Operation.Delete.of(contentKey))
            .commitWithResponse();

    @SuppressWarnings("resource")
    PrintWriter writer = cli.writer();
    writer.println(
        new AttributedStringBuilder()
            .append("Dropped ")
            .append(kind)
            .append(" ")
            .append(contentKey.toPathString(), AttributedStyle.BOLD));

    return committed;
  }

  public String name() {
    return Token.TokenType.DROP
        + " "
        + Token.TokenType.TABLE
        + "/"
        + Token.TokenType.VIEW
        + "/"
        + Token.TokenType.NAMESPACE;
  }

  public String description() {
    return "Drops a table or view.";
  }

  @Override
  public List<List<Node.NodeType>> matchesNodeTypes() {
    return List.of(
        List.of(Token.TokenType.DROP),
        List.of(Token.TokenType.DROP, Token.TokenType.TABLE),
        List.of(Token.TokenType.DROP, Token.TokenType.VIEW),
        List.of(Token.TokenType.DROP, Token.TokenType.NAMESPACE));
  }
}

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
import org.projectnessie.client.api.CommitMultipleOperationsBuilder;
import org.projectnessie.model.Branch;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.CommitResponse;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.Namespace;
import org.projectnessie.model.Operation;
import org.projectnessie.nessie.cli.cli.BaseNessieCli;
import org.projectnessie.nessie.cli.cmdspec.CreateNamespaceCommandSpec;
import org.projectnessie.nessie.cli.grammar.Node;
import org.projectnessie.nessie.cli.grammar.Token;

public class CreateNamespaceCommand extends NessieCommittingCommand<CreateNamespaceCommandSpec> {
  public CreateNamespaceCommand() {}

  @Override
  protected CommitResponse executeCommitting(
      @Nonnull BaseNessieCli cli,
      CreateNamespaceCommandSpec spec,
      Branch branch,
      CommitMultipleOperationsBuilder commit)
      throws Exception {
    ContentKey contentKey = ContentKey.fromPathString(spec.getNamespace());

    Namespace namespace = Namespace.of(spec.setProperties(), contentKey.getElementsArray());

    CommitResponse committed =
        commit
            .commitMeta(CommitMeta.fromMessage("Create namespace " + contentKey))
            .operation(Operation.Put.of(contentKey, namespace))
            .commitWithResponse();

    @SuppressWarnings("resource")
    PrintWriter writer = cli.writer();
    writer.println(
        new AttributedStringBuilder()
            .append("Created namespace ")
            .append(contentKey.toPathString(), AttributedStyle.BOLD));

    return committed;
  }

  public String name() {
    return Token.TokenType.CREATE + " " + Token.TokenType.NAMESPACE;
  }

  public String description() {
    return "Create a new namespace.";
  }

  @Override
  public List<List<Node.NodeType>> matchesNodeTypes() {
    return List.of(
        List.of(Token.TokenType.CREATE),
        List.of(Token.TokenType.CREATE, Token.TokenType.NAMESPACE));
  }
}

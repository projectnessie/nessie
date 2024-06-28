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
import java.util.HashMap;
import java.util.List;
import org.jline.utils.AttributedStringBuilder;
import org.jline.utils.AttributedStyle;
import org.projectnessie.client.api.CommitMultipleOperationsBuilder;
import org.projectnessie.model.Branch;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.CommitResponse;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.ImmutableNamespace;
import org.projectnessie.model.Namespace;
import org.projectnessie.model.Operation;
import org.projectnessie.nessie.cli.cli.BaseNessieCli;
import org.projectnessie.nessie.cli.cmdspec.AlterNamespaceCommandSpec;
import org.projectnessie.nessie.cli.grammar.Node;
import org.projectnessie.nessie.cli.grammar.Token;

public class AlterNamespaceCommand extends NessieCommittingCommand<AlterNamespaceCommandSpec> {
  public AlterNamespaceCommand() {}

  @Override
  protected CommitResponse executeCommitting(
      @Nonnull BaseNessieCli cli,
      AlterNamespaceCommandSpec spec,
      Branch branch,
      CommitMultipleOperationsBuilder commit)
      throws Exception {
    ContentKey contentKey = ContentKey.fromPathString(spec.getNamespace());

    @SuppressWarnings("resource")
    Content content =
        applyReference(cli, spec, cli.mandatoryNessieApi().getContent())
            .getSingle(contentKey)
            .getContent();
    if (!(content instanceof Namespace)) {
      throw new IllegalArgumentException(
          "Key " + contentKey + " is not a namespace, but a " + content.getType());
    }

    Namespace namespace = (Namespace) content;

    HashMap<String, String> props = new HashMap<>(namespace.getProperties());
    spec.removeProperties().forEach(props::remove);
    props.putAll(spec.setProperties());

    ImmutableNamespace.Builder builder = Namespace.builder().from(namespace);
    builder.properties(props);
    namespace = builder.build();

    CommitResponse committed =
        commit
            .commitMeta(CommitMeta.fromMessage("Alter namespace " + contentKey))
            .operation(Operation.Put.of(contentKey, namespace))
            .commitWithResponse();

    @SuppressWarnings("resource")
    PrintWriter writer = cli.writer();
    writer.println(
        new AttributedStringBuilder()
            .append("Updated namespace ")
            .append(contentKey.toPathString(), AttributedStyle.BOLD));

    return committed;
  }

  public String name() {
    return Token.TokenType.ALTER + " " + Token.TokenType.NAMESPACE;
  }

  public String description() {
    return "Changes a new namespace.";
  }

  @Override
  public List<List<Node.NodeType>> matchesNodeTypes() {
    return List.of(
        List.of(Token.TokenType.ALTER), List.of(Token.TokenType.ALTER, Token.TokenType.NAMESPACE));
  }
}

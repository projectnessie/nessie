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

import static java.util.Objects.requireNonNull;
import static org.projectnessie.nessie.cli.cli.BaseNessieCli.STYLE_BOLD;
import static org.projectnessie.nessie.cli.cli.BaseNessieCli.STYLE_ERROR;
import static org.projectnessie.nessie.cli.cli.BaseNessieCli.STYLE_FAIL;
import static org.projectnessie.nessie.cli.cli.BaseNessieCli.STYLE_FAINT;
import static org.projectnessie.nessie.cli.cli.BaseNessieCli.STYLE_INFO;

import jakarta.annotation.Nonnull;
import java.io.PrintWriter;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;
import org.jline.terminal.Terminal;
import org.jline.utils.AttributedString;
import org.jline.utils.AttributedStringBuilder;
import org.projectnessie.client.api.CommitMultipleOperationsBuilder;
import org.projectnessie.model.Branch;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.CommitResponse;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.GetMultipleContentsResponse;
import org.projectnessie.model.Operation;
import org.projectnessie.model.Reference;
import org.projectnessie.nessie.cli.cli.BaseNessieCli;
import org.projectnessie.nessie.cli.cli.CliCommandFailedException;
import org.projectnessie.nessie.cli.cmdspec.RevertContentCommandSpec;
import org.projectnessie.nessie.cli.grammar.Node;
import org.projectnessie.nessie.cli.grammar.Token;
import org.projectnessie.nessie.cli.jsonpretty.JsonPretty;

public class RevertContentCommand extends NessieCommittingCommand<RevertContentCommandSpec> {

  @Override
  protected CommitResponse executeCommitting(
      @Nonnull BaseNessieCli cli,
      RevertContentCommandSpec spec,
      Branch branch,
      CommitMultipleOperationsBuilder commit)
      throws Exception {

    List<ContentKey> contentKeys =
        spec.getContentKeys().stream().map(ContentKey::fromPathString).collect(Collectors.toList());

    String refName =
        spec.getSourceRef() != null ? spec.getSourceRef() : cli.getCurrentReference().getName();
    @SuppressWarnings("resource")
    GetMultipleContentsResponse contents =
        cli.mandatoryNessieApi()
            .getContent()
            .refName(refName)
            .hashOnRef(spec.getSourceRefTimestampOrHash())
            .keys(contentKeys)
            .getWithResponse();

    Terminal terminal = cli.terminal();
    PrintWriter writer = cli.writer();

    Map<ContentKey, Content> contentsMap = contents.toContentsMap();
    Reference effectiveReference =
        requireNonNull(
            contents.getEffectiveReference(), "No effective reference, Nessie API v2 required");
    boolean fail = false;
    for (ContentKey contentKey : contentKeys) {
      Content content = contentsMap.get(contentKey);
      if (content != null) {
        commit.operation(Operation.Put.of(contentKey, content));
      } else if (spec.isAllowDeletes()) {
        commit.operation(Operation.Delete.of(contentKey));
      } else {
        writer.println(
            new AttributedStringBuilder()
                .append("Content key ", STYLE_ERROR)
                .append(contentKey.toPathString(), STYLE_FAIL)
                .append(" does not exist and ALLOW DELETES clause was not specified.", STYLE_ERROR)
                .toAnsi(terminal));
        fail = true;
      }
    }
    if (fail) {
      throw new CliCommandFailedException();
    }

    String refStr =
        String.format(
            "%s %s at %s",
            effectiveReference.getType().name().toLowerCase(Locale.ROOT),
            effectiveReference.getName(),
            effectiveReference.getHash());

    CommitResponse committed;
    String prevHash;
    if (spec.isDryRun()) {
      committed = null;
      prevHash = branch.getHash();
    } else {
      committed =
          commit
              .commitMeta(
                  CommitMeta.fromMessage("Revert " + contentKeys + " to state on " + refStr))
              .commitWithResponse();
      prevHash = committed.getTargetBranch().getHash() + "~1";
    }

    @SuppressWarnings("resource")
    GetMultipleContentsResponse previousContentsResponse =
        cli.mandatoryNessieApi()
            .getContent()
            .refName(branch.getName())
            .hashOnRef(prevHash)
            .keys(contentKeys)
            .getWithResponse();

    Reference prevRef =
        requireNonNull(previousContentsResponse.getEffectiveReference(), "Require Nessie API v2");

    writer.println(
        new AttributedStringBuilder()
            .append("Reverted content keys ")
            .append(
                contentKeys.stream()
                    .map(ContentKey::toPathString)
                    .collect(Collectors.joining(", ")),
                STYLE_BOLD)
            .append(" to state on ")
            .append(refStr, STYLE_BOLD)
            .append(" from hash ")
            .append(prevRef.getHash(), STYLE_BOLD)
            .append(" :"));

    JsonPretty jsonPretty = new JsonPretty(terminal, writer);

    Map<ContentKey, Content> previousContents = previousContentsResponse.toContentsMap();
    for (ContentKey contentKey : contentKeys) {
      Content content = contentsMap.get(contentKey);
      if (content != null) {
        writer.println(
            new AttributedStringBuilder()
                .append("  Key ", STYLE_FAINT)
                .append(contentKey.toPathString(), STYLE_BOLD)
                .append(" updated from", STYLE_FAINT)
                .toAnsi(terminal));
        jsonPretty.prettyPrintObject(previousContents.get(contentKey));
        writer.println(
            new AttributedStringBuilder().append("    to ", STYLE_FAINT).toAnsi(terminal));
        jsonPretty.prettyPrintObject(content);
      } else if (spec.isAllowDeletes()) {
        writer.println(
            new AttributedStringBuilder()
                .append("  Key ", STYLE_FAINT)
                .append(contentKey.toPathString(), STYLE_BOLD)
                .append(" was deleted", STYLE_FAINT)
                .toAnsi(terminal));
      }
    }

    if (spec.isDryRun()) {
      writer.println(
          new AttributedString("Dry run, not committing any changes.", STYLE_INFO)
              .toAnsi(terminal));
    }
    return committed;
  }

  public String name() {
    return Token.TokenType.REVERT + " " + Token.TokenType.CONTENT;
  }

  public String description() {
    return "Create a new namespace.";
  }

  @Override
  public List<List<Node.NodeType>> matchesNodeTypes() {
    return List.of(
        List.of(Token.TokenType.REVERT), List.of(Token.TokenType.REVERT, Token.TokenType.CONTENT));
  }
}

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
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import org.jline.terminal.Terminal;
import org.jline.utils.AttributedStringBuilder;
import org.projectnessie.client.api.MergeReferenceBuilder;
import org.projectnessie.client.api.NessieApiV2;
import org.projectnessie.model.Branch;
import org.projectnessie.model.Conflict;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.Detached;
import org.projectnessie.model.MergeBehavior;
import org.projectnessie.model.MergeResponse;
import org.projectnessie.model.Reference;
import org.projectnessie.model.Tag;
import org.projectnessie.nessie.cli.cli.BaseNessieCli;
import org.projectnessie.nessie.cli.cli.CliCommandFailedException;
import org.projectnessie.nessie.cli.cmdspec.MergeBranchCommandSpec;
import org.projectnessie.nessie.cli.grammar.Node;
import org.projectnessie.nessie.cli.grammar.Token;

public class MergeBranchCommand extends NessieCommand<MergeBranchCommandSpec> {

  public MergeBranchCommand() {}

  @Override
  public void execute(@Nonnull BaseNessieCli cli, MergeBranchCommandSpec spec) throws Exception {
    @SuppressWarnings("resource")
    NessieApiV2 api = cli.mandatoryNessieApi();

    String fromRefName = spec.getRef();
    Reference fromRef = api.getReference().refName(fromRefName).get();
    if (spec.getRefTimestampOrHash() != null) {
      if (fromRef instanceof Branch) {
        fromRef = Branch.of(fromRefName, spec.getRefTimestampOrHash());
      } else if (fromRef instanceof Tag) {
        fromRef = Tag.of(fromRefName, spec.getRefTimestampOrHash());
      } else {
        fromRef = Detached.of(spec.getRefTimestampOrHash());
      }
    }

    StringBuilder msg = new StringBuilder().append("Merge ");
    if (fromRef instanceof Branch) {
      msg.append("branch ").append(fromRef.getName());
    } else if (fromRef instanceof Tag) {
      msg.append("tag ").append(fromRef.getName());
    } else {
      msg.append("commit ").append(fromRef.getHash());
    }

    Reference target = cli.getCurrentReference();
    if (spec.getInto() != null) {
      target = api.getReference().refName(spec.getInto()).get();
    }
    if (!(target instanceof Branch)) {
      throw new IllegalStateException("Cannot commit to non-branch reference " + target.getName());
    }

    msg.append(" into ").append(target.getName());

    MergeReferenceBuilder merge =
        api.mergeRefIntoBranch()
            .fromRef(fromRef)
            .branch((Branch) target)
            .dryRun(spec.isDryRun())
            .returnConflictAsResult(true);

    String briefMessage = msg.toString();

    Map<String, String> keyMergeBehaviors = spec.getKeyMergeBehaviors();
    if (spec.getDefaultMergeBehavior() != null || !keyMergeBehaviors.isEmpty()) {
      msg.append("\n\n");
    }
    if (spec.getDefaultMergeBehavior() != null) {
      merge.defaultMergeMode(MergeBehavior.valueOf(spec.getDefaultMergeBehavior()));
      msg.append(" using default merge behavior ").append(spec.getDefaultMergeBehavior());
    }

    keyMergeBehaviors.forEach(
        (k, b) -> {
          merge.mergeMode(ContentKey.fromPathString(k), MergeBehavior.valueOf(b));
          msg.append(", key ")
              .append(k)
              .append(" behavior ")
              .append(spec.getDefaultMergeBehavior());
        });

    MergeResponse response = merge.merge();

    Branch newTarget = Branch.of(target.getName(), response.getResultantTargetHash());
    if (response.wasApplied()) {
      cli.setCurrentReference(newTarget);
    }

    Terminal terminal = cli.terminal();
    @SuppressWarnings("resource")
    PrintWriter writer = cli.writer();
    String detailsHeading;
    if (response.wasSuccessful()) {
      writer.println(
          briefMessage
              + (response.wasApplied()
                  ? " successfully committed."
                  : (spec.isDryRun()
                      ? "checked using dry run (not committed)."
                      : " did not change target branch.")));

      detailsHeading = "\nPer-key details:\n";
    } else {
      writer.println(
          new AttributedStringBuilder()
              .append(briefMessage, BaseNessieCli.STYLE_FAIL)
              .append(" FAILED.", BaseNessieCli.STYLE_FAIL)
              .toAnsi(terminal));

      detailsHeading = "\nConflicts:\n";
    }

    if (!response.getDetails().isEmpty()) {
      writer.println(detailsHeading);

      Comparator<MergeResponse.ContentKeyDetails> conflictsFirstComparator =
          Comparator.comparing(d -> d.getConflict() != null ? 0 : 1);
      Comparator<MergeResponse.ContentKeyDetails> keyComparator =
          Comparator.comparing(MergeResponse.ContentKeyDetails::getKey);

      response.getDetails().stream()
          .sorted(conflictsFirstComparator.thenComparing(keyComparator))
          .forEach(
              detail -> {
                AttributedStringBuilder keyInfo =
                    new AttributedStringBuilder()
                        .append(
                            format("%-70s ", detail.getKey().toString()), BaseNessieCli.STYLE_KEY)
                        .append(
                            format("%-7s", detail.getMergeBehavior().name()),
                            BaseNessieCli.STYLE_FAINT);
                Conflict conflict = detail.getConflict();
                if (conflict != null) {
                  keyInfo
                      .append(" ")
                      .append(conflict.message(), BaseNessieCli.STYLE_FAIL)
                      .append(" (", BaseNessieCli.STYLE_FAINT)
                      .append(conflict.conflictType().name(), BaseNessieCli.STYLE_ERROR)
                      .append(")", BaseNessieCli.STYLE_FAINT);
                } else {
                  keyInfo.append(" ").append("OK", BaseNessieCli.STYLE_SUCCESS);
                }
                writer.println(keyInfo.toAnsi(terminal));
              });
    }

    if (response.wasApplied()) {
      writer.println(
          new AttributedStringBuilder()
              .append("Target branch ")
              .append(newTarget.getName(), BaseNessieCli.STYLE_SUCCESS)
              .append(" is now at commit ")
              .append(newTarget.getHash(), BaseNessieCli.STYLE_INFO)
              .toAnsi(cli.terminal()));
    }

    if (!response.wasSuccessful()) {
      throw new CliCommandFailedException();
    }
  }

  public String name() {
    return Token.TokenType.MERGE + " " + Token.TokenType.BRANCH;
  }

  public String description() {
    return "Merge a reference into a branch.";
  }

  @Override
  public List<List<Node.NodeType>> matchesNodeTypes() {
    return List.of(List.of(Token.TokenType.MERGE));
  }
}

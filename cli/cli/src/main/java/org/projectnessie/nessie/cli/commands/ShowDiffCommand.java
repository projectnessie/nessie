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
import static org.projectnessie.nessie.cli.cli.BaseNessieCli.STYLE_ERROR;
import static org.projectnessie.nessie.cli.cli.BaseNessieCli.STYLE_FAINT;
import static org.projectnessie.nessie.cli.cli.BaseNessieCli.STYLE_GREEN;
import static org.projectnessie.nessie.cli.cli.BaseNessieCli.STYLE_YELLOW;

import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.format.DateTimeParseException;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.jline.utils.AttributedStringBuilder;
import org.projectnessie.client.api.GetDiffBuilder;
import org.projectnessie.client.api.NessieApiV2;
import org.projectnessie.model.DiffResponse.DiffEntry;
import org.projectnessie.nessie.cli.cli.BaseNessieCli;
import org.projectnessie.nessie.cli.cmdspec.ShowDiffCommandSpec;
import org.projectnessie.nessie.cli.grammar.Node;
import org.projectnessie.nessie.cli.grammar.Token;

/**
 * {@code SHOW DIFF [FROM [ref]] TO ref} - prints, {@code git diff}-style, which keys were added,
 * removed or changed between two references. Unlike {@code SHOW LOG} (which walks the commit
 * history of a single reference) this compares the *content* of two references directly,
 * regardless of whether one is an ancestor of the other.
 */
public class ShowDiffCommand extends NessieListingCommand<ShowDiffCommandSpec> {

  public ShowDiffCommand() {}

  @Override
  protected Stream<String> executeListing(BaseNessieCli cli, ShowDiffCommandSpec spec)
      throws Exception {
    @SuppressWarnings("resource")
    NessieApiV2 api = cli.mandatoryNessieApi();

    String fromRefName =
        spec.getFromRef() != null ? spec.getFromRef() : cli.getCurrentReference().getName();
    String toRefName = spec.getToRef();

    GetDiffBuilder diffBuilder =
        api.getDiff()
            .fromRefName(fromRefName)
            .fromHashOnRef(hashOrTimestamp(spec.getFromTimestampOrHash()))
            .toRefName(toRefName)
            .toHashOnRef(hashOrTimestamp(spec.getToTimestampOrHash()));

    Stream<String> headerLine =
        Stream.of(
            new AttributedStringBuilder()
                .append(format("diff --nessie a/%s b/%s", fromRefName, toRefName), STYLE_FAINT)
                .toAnsi(cli.terminal()),
            "");

    Stream<String> entryLines =
        diffBuilder.stream()
            .sorted(Comparator.comparing(DiffEntry::getKey))
            .map(entry -> formatEntry(cli, entry));

    return Stream.concat(headerLine, entryLines);
  }

  private String formatEntry(BaseNessieCli cli, DiffEntry entry) {
    AttributedStringBuilder line = new AttributedStringBuilder();
    if (entry.getFrom() == null) {
      line.append(format("A  %-70s ", entry.getKey()), STYLE_GREEN)
          .append(entry.getTo().getType().name(), STYLE_FAINT);
    } else if (entry.getTo() == null) {
      line.append(format("D  %-70s ", entry.getKey()), STYLE_ERROR)
          .append(entry.getFrom().getType().name(), STYLE_FAINT);
    } else {
      line.append(format("M  %-70s ", entry.getKey()), STYLE_YELLOW)
          .append(entry.getTo().getType().name(), STYLE_FAINT);
    }
    return line.toAnsi(cli.terminal());
  }

  @SuppressWarnings("JavaInstantGetSecondsGetNano")
  private static String hashOrTimestamp(String hash) {
    if (hash == null) {
      return null;
    }
    try {
      Instant instant = ZonedDateTime.parse(hash).toInstant();
      long millis = instant.toEpochMilli();
      // Silently add one millisecond if a fraction of a millisecond has been specified, because
      // we can only use millisecond precision via
      // org.projectnessie.client.api.OnReferenceBuilder.hashOnRef.
      if (instant.getNano() % TimeUnit.MILLISECONDS.toNanos(1) != 0) {
        millis++;
      }
      return "*" + millis;
    } catch (DateTimeParseException e) {
      return hash;
    }
  }

  @Override
  public String name() {
    return Token.TokenType.SHOW + " " + Token.TokenType.DIFF;
  }

  @Override
  public String description() {
    return "Show the differences in content between two references, git-diff style.";
  }

  @Override
  public List<List<Node.NodeType>> matchesNodeTypes() {
    return List.of(
        List.of(Token.TokenType.SHOW), List.of(Token.TokenType.SHOW, Token.TokenType.DIFF));
  }
}

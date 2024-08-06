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
import static java.util.Objects.requireNonNull;
import static org.projectnessie.nessie.cli.cli.BaseNessieCli.STYLE_BOLD;
import static org.projectnessie.nessie.cli.cli.BaseNessieCli.STYLE_FAINT;

import jakarta.annotation.Nonnull;
import java.io.PrintWriter;
import java.util.List;
import org.jline.utils.AttributedStringBuilder;
import org.projectnessie.client.api.GetEntriesBuilder;
import org.projectnessie.client.api.NessieApiV2;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.EntriesResponse;
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

    GetEntriesBuilder getAny =
        api.getEntries().key(ContentKey.of("non", "existing")).maxRecords(1).refName(spec.getRef());
    if (spec.getRefTimestampOrHash() != null) {
      getAny.hashOnRef(spec.getRefTimestampOrHash());
    }
    EntriesResponse response = getAny.get();
    Reference ref = requireNonNull(response.getEffectiveReference());

    String refType = spec.getRefType();
    if (refType != null && !"REFERENCE".equals(refType)) {
      if (Reference.ReferenceType.valueOf(refType) != ref.getType()) {
        throw new IllegalArgumentException(
            format("'%s' is not a %s but a %s.", spec.getRef(), refType, ref.getType()));
      }
    }

    cli.setCurrentReference(ref);

    @SuppressWarnings("resource")
    PrintWriter writer = cli.writer();
    writer.println(
        new AttributedStringBuilder()
            .append("Using ")
            .append(ref.getName(), STYLE_BOLD)
            .append(" at ")
            .append(ref.getHash(), STYLE_FAINT));
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

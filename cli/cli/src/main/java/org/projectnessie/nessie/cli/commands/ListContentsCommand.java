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
import static org.projectnessie.nessie.cli.cli.BaseNessieCli.STYLE_FAINT;

import java.util.List;
import java.util.stream.Stream;
import org.jline.utils.AttributedStringBuilder;
import org.projectnessie.client.api.GetEntriesBuilder;
import org.projectnessie.nessie.cli.cli.BaseNessieCli;
import org.projectnessie.nessie.cli.cmdspec.ListContentsCommandSpec;
import org.projectnessie.nessie.cli.grammar.Node;
import org.projectnessie.nessie.cli.grammar.Token;

public class ListContentsCommand extends NessieListingCommand<ListContentsCommandSpec> {
  public ListContentsCommand() {}

  @Override
  protected Stream<String> executeListing(BaseNessieCli cli, ListContentsCommandSpec spec)
      throws Exception {

    String filter = null;
    if (spec.getFilter() != null) {
      filter = spec.getFilter();
    } else {
      if (spec.getStartsWith() != null) {
        filter = format("entry.encodedKey.startsWith('%s')", spec.getStartsWith());
      }
      if (spec.getContains() != null) {
        String f = format("entry.encodedKey.contains('%s')", spec.getContains());
        filter = filter == null ? f : format("(%s && %s)", filter, f);
      }
    }

    @SuppressWarnings("resource")
    GetEntriesBuilder entries = applyReference(cli, spec, cli.mandatoryNessieApi().getEntries());

    if (filter != null) {
      entries.filter(filter);
    }

    return entries.stream()
        .map(
            e ->
                new AttributedStringBuilder()
                    .append(format("%15s ", e.getType()), STYLE_FAINT)
                    .append(e.getName().toString())
                    .toAnsi(cli.terminal()));
  }

  public String name() {
    return Token.TokenType.LIST + " " + Token.TokenType.CONTENTS;
  }

  public String description() {
    return "List contents.";
  }

  @Override
  public List<List<Node.NodeType>> matchesNodeTypes() {
    return List.of(
        List.of(Token.TokenType.LIST), List.of(Token.TokenType.LIST, Token.TokenType.CONTENTS));
  }
}

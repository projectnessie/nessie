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
import org.projectnessie.client.api.GetAllReferencesBuilder;
import org.projectnessie.model.FetchOption;
import org.projectnessie.nessie.cli.cli.BaseNessieCli;
import org.projectnessie.nessie.cli.cmdspec.ListReferencesCommandSpec;
import org.projectnessie.nessie.cli.grammar.Node;
import org.projectnessie.nessie.cli.grammar.Token;

public class ListReferencesCommand extends NessieListingCommand<ListReferencesCommandSpec> {
  public ListReferencesCommand() {}

  @Override
  protected Stream<String> executeListing(BaseNessieCli cli, ListReferencesCommandSpec spec)
      throws Exception {
    FetchOption fetchOption = FetchOption.MINIMAL;

    String filter = null;
    if (spec.getFilter() != null) {
      filter = spec.getFilter();
    } else {
      if (spec.getStartsWith() != null) {
        filter = format("ref.name.startsWith('%s')", spec.getStartsWith());
      }
      if (spec.getContains() != null) {
        String f = format("ref.name.contains('%s')", spec.getContains());
        filter = filter == null ? f : format("(%s && %s)", filter, f);
      }
    }

    @SuppressWarnings("resource")
    GetAllReferencesBuilder refs = cli.mandatoryNessieApi().getAllReferences().fetch(fetchOption);

    if (filter != null) {
      refs.filter(filter);
    }

    return refs.stream()
        .flatMap(
            ref ->
                Stream.of(
                    new AttributedStringBuilder()
                        .append(format(" %-6s ", ref.getType().name()), STYLE_FAINT)
                        .append(ref.getName())
                        .append(format(" @ %s", ref.getHash()), STYLE_FAINT)
                        .toAnsi(cli.terminal())));
  }

  public String name() {
    return Token.TokenType.LIST + " " + Token.TokenType.REFERENCES;
  }

  public String description() {
    return "List named references (branches and tags).";
  }

  @Override
  public List<List<Node.NodeType>> matchesNodeTypes() {
    return List.of(
        List.of(Token.TokenType.LIST), List.of(Token.TokenType.LIST, Token.TokenType.REFERENCES));
  }
}

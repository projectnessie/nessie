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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.Nonnull;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.RESTSerializers;
import org.apache.iceberg.view.BaseView;
import org.jline.terminal.Terminal;
import org.jline.utils.AttributedString;
import org.jline.utils.AttributedStringBuilder;
import org.jline.utils.AttributedStyle;
import org.projectnessie.client.api.NessieApiV2;
import org.projectnessie.error.NessieContentNotFoundException;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.GetMultipleContentsResponse;
import org.projectnessie.nessie.cli.cli.BaseNessieCli;
import org.projectnessie.nessie.cli.cmdspec.ShowContentCommandSpec;
import org.projectnessie.nessie.cli.grammar.Node;
import org.projectnessie.nessie.cli.grammar.Token;
import org.projectnessie.nessie.cli.jsonpretty.JsonPretty;

public class ShowContentCommand extends NessieListingCommand<ShowContentCommandSpec> {
  public ShowContentCommand() {}

  @Override
  protected Stream<String> executeListing(@Nonnull BaseNessieCli cli, ShowContentCommandSpec spec)
      throws Exception {

    cli.verifyAnyConnected();

    @SuppressWarnings("resource")
    NessieApiV2 api = cli.mandatoryNessieApi();

    ContentKey key = ContentKey.fromPathString(spec.getContentKey());
    GetMultipleContentsResponse response =
        applyReference(cli, spec, api.getContent()).key(key).getWithResponse();

    Content content = response.toContentsMap().get(key);
    if (content == null) {
      throw new NessieContentNotFoundException(
          key,
          response.getEffectiveReference().getName()
              + "@"
              + response.getEffectiveReference().getHash());
    }

    Terminal terminal = cli.terminal();

    StringWriter sw = new StringWriter();
    PrintWriter writer = new PrintWriter(sw);
    try {
      writer.println(
          new AttributedStringBuilder()
              .append("Content type: ")
              .append(content.getType().name(), STYLE_FAINT)
              .toAnsi(terminal));
      writer.println(
          new AttributedStringBuilder()
              .append(" Content Key: ")
              .append(key.toPathString(), STYLE_BOLD)
              .toAnsi(terminal));
      writer.println(
          new AttributedStringBuilder()
              .append("          ID: ")
              .append(content.getId(), STYLE_FAINT)
              .toAnsi(terminal));
      terminal.writer().println("        JSON: ");

      JsonPretty jsonPretty = new JsonPretty(terminal, writer);

      writer.println();
      writer.println(
          new AttributedString("Nessie metadata:", AttributedStyle.BOLD.underline())
              .toAnsi(terminal));

      String jsonString =
          new ObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(content);
      jsonPretty.prettyPrint(jsonString);

      // Iceberg

      cli.icebergClient()
          .ifPresent(
              iceberg -> {
                Object metadata;
                switch (spec.getContentKind()) {
                  case "TABLE":
                    metadata =
                        ((BaseTable) iceberg.loadTable(TableIdentifier.of(key.getElementsArray())))
                            .operations()
                            .current();
                    break;
                  case "VIEW":
                    metadata =
                        ((BaseView) iceberg.loadView(TableIdentifier.of(key.getElementsArray())))
                            .operations()
                            .current();
                    break;
                  case "NAMESPACE":
                    metadata = iceberg.loadNamespaceMetadata(Namespace.of(key.getElementsArray()));
                    break;
                  default:
                    return;
                }

                writer.println();
                writer.println(
                    new AttributedString("Iceberg metadata:", AttributedStyle.BOLD.underline())
                        .toAnsi(terminal));

                try {
                  ObjectMapper mapper = new ObjectMapper();
                  RESTSerializers.registerAll(mapper);

                  String metaJsonString =
                      mapper.writerWithDefaultPrettyPrinter().writeValueAsString(metadata);

                  jsonPretty.prettyPrint(metaJsonString);
                } catch (JsonProcessingException e) {
                  throw new RuntimeException(e);
                }
              });
    } finally {
      writer.flush();
    }

    return Arrays.stream(sw.toString().split("\n"));
  }

  public String name() {
    return Token.TokenType.SHOW
        + " "
        + Token.TokenType.TABLE
        + "/"
        + Token.TokenType.VIEW
        + "/"
        + Token.TokenType.NAMESPACE;
  }

  public String description() {
    return "Show content.";
  }

  @Override
  public List<List<Node.NodeType>> matchesNodeTypes() {
    return List.of(
        List.of(Token.TokenType.SHOW),
        List.of(Token.TokenType.SHOW, Token.TokenType.TABLE),
        List.of(Token.TokenType.SHOW, Token.TokenType.VIEW),
        List.of(Token.TokenType.SHOW, Token.TokenType.NAMESPACE));
  }
}

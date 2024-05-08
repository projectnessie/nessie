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
package org.projectnessie.nessie.cli.cli;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static org.jline.utils.AttributedStyle.BLUE;
import static org.jline.utils.AttributedStyle.CYAN;
import static org.jline.utils.AttributedStyle.GREEN;
import static org.jline.utils.AttributedStyle.RED;
import static org.jline.utils.AttributedStyle.YELLOW;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.UncheckedIOException;
import java.lang.reflect.Field;
import java.net.URL;
import java.net.URLEncoder;
import java.util.Optional;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.rest.RESTSessionCatalog;
import org.apache.iceberg.rest.ResourcePaths;
import org.jline.terminal.Terminal;
import org.jline.utils.AttributedStringBuilder;
import org.jline.utils.AttributedStyle;
import org.projectnessie.client.api.NessieApiV2;
import org.projectnessie.model.Reference;
import org.projectnessie.nessie.cli.grammar.NessieCliLexer;
import org.projectnessie.nessie.cli.grammar.NessieCliParser;
import org.projectnessie.nessie.cli.grammar.Token;

public abstract class BaseNessieCli {

  public static final AttributedStyle STYLE_ERROR = AttributedStyle.DEFAULT.foreground(RED);
  public static final AttributedStyle STYLE_FAIL = AttributedStyle.DEFAULT.bold().foreground(RED);
  public static final AttributedStyle STYLE_FAINT = AttributedStyle.DEFAULT.faint();
  public static final AttributedStyle STYLE_YELLOW = AttributedStyle.DEFAULT.foreground(YELLOW);
  public static final AttributedStyle STYLE_GREEN = AttributedStyle.DEFAULT.foreground(GREEN);
  public static final AttributedStyle STYLE_BLUE = AttributedStyle.DEFAULT.foreground(BLUE);
  public static final AttributedStyle STYLE_SUCCESS = AttributedStyle.DEFAULT.foreground(GREEN);
  public static final AttributedStyle STYLE_BOLD = AttributedStyle.DEFAULT.italic().bold();
  public static final AttributedStyle STYLE_KEY = AttributedStyle.BOLD;
  public static final AttributedStyle STYLE_COMMAND_NAME = AttributedStyle.BOLD;
  public static final AttributedStyle STYLE_INFO = AttributedStyle.DEFAULT.foreground(CYAN);
  public static final AttributedStyle STYLE_ITALIC_BOLD = AttributedStyle.DEFAULT.italic().bold();
  public static final AttributedStyle STYLE_UNDERLINE_BOLD =
      AttributedStyle.DEFAULT.underline().bold();
  public static final AttributedStyle STYLE_HEADING = STYLE_UNDERLINE_BOLD;

  private Integer exitWithCode;
  private NessieApiV2 nessieApi;
  private RESTCatalog icebergClient;
  private Reference currentReference;
  private Terminal terminal;

  public Integer getExitWithCode() {
    return exitWithCode;
  }

  public void setExitWithCode(int exitCode) {
    exitWithCode = exitCode;
  }

  public void setTerminal(Terminal terminal) {
    this.terminal = terminal;
  }

  public PrintWriter writer() {
    return terminal.writer();
  }

  public void exitRepl(int exitCode) {
    exitWithCode = exitCode;
  }

  public Terminal terminal() {
    return terminal;
  }

  public boolean dumbTerminal() {
    return terminal.getType().equals("dumb");
  }

  public void setCurrentReference(Reference currentReference) {
    this.currentReference = currentReference;

    if (icebergClient != null) {
      try {
        String icebergRestPrefix = URLEncoder.encode(currentReference.getName(), UTF_8);

        Field sessionCatalogField = icebergClient.getClass().getDeclaredField("sessionCatalog");
        sessionCatalogField.setAccessible(true);
        RESTSessionCatalog sessionCatalog =
            (RESTSessionCatalog) sessionCatalogField.get(icebergClient);

        Field pathsField = sessionCatalog.getClass().getDeclaredField("paths");
        pathsField.setAccessible(true);
        ResourcePaths paths = new ResourcePaths(icebergRestPrefix);
        pathsField.set(sessionCatalog, paths);

      } catch (IllegalAccessException | NoSuchFieldException e) {
        throw new RuntimeException(e);
      }
    }
  }

  public Reference getCurrentReference() {
    if (nessieApi == null) {
      throw new NotConnectedException();
    }
    return currentReference;
  }

  public void connected(NessieApiV2 nessieApi, RESTCatalog icebergClient) {
    if (this.nessieApi != null) {
      try {
        this.nessieApi.close();
      } catch (Exception e) {
        @SuppressWarnings("resource")
        PrintWriter writer = writer();
        writer.println(
            new AttributedStringBuilder()
                .append("Failed to close the existing client: ")
                .append(e.toString(), STYLE_ERROR));
      }
    }
    if (this.icebergClient != null) {
      try {
        this.icebergClient.close();
      } catch (Exception e) {
        @SuppressWarnings("resource")
        PrintWriter writer = writer();
        writer.println(
            new AttributedStringBuilder()
                .append("Failed to close the existing client: ")
                .append(e.toString(), STYLE_ERROR));
      }
    }
    this.nessieApi = nessieApi;
    this.icebergClient = icebergClient;
  }

  public Optional<NessieApiV2> nessieApi() {
    return Optional.ofNullable(nessieApi);
  }

  public NessieApiV2 mandatoryNessieApi() {
    if (nessieApi == null) {
      throw new NotConnectedException();
    }
    return nessieApi;
  }

  public Optional<RESTCatalog> icebergClient() {
    return Optional.ofNullable(icebergClient);
  }

  public RESTCatalog mandatoryIcebergClient() {
    if (icebergClient == null) {
      throw new NotConnectedException();
    }
    return icebergClient;
  }

  public NessieCliParser newParserForSource(String source) {
    NessieCliLexer lexer = new NessieCliLexer(source);
    NessieCliParser parser = new NessieCliParser(lexer);
    parser.deactivateTokenType(Token.TokenType.IN);
    return parser;
  }

  public void verifyAnyConnected() {
    if (nessieApi == null && icebergClient == null) {
      throw new NotConnectedException();
    }
  }

  public String readResource(String resource) {
    URL url = NessieCliImpl.class.getResource(resource);
    try (InputStream in =
        requireNonNull(url, "Could not open resource from classpath " + resource)
            .openConnection()
            .getInputStream()) {
      return new String(in.readAllBytes(), UTF_8);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}

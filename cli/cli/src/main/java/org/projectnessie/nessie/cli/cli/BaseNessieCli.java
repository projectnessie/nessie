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

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.UncheckedIOException;
import java.net.URL;
import java.util.Optional;
import org.jline.terminal.Terminal;
import org.jline.utils.AttributedStringBuilder;
import org.jline.utils.AttributedStyle;
import org.projectnessie.client.api.NessieApiV2;
import org.projectnessie.model.Reference;

public abstract class BaseNessieCli {

  public static final AttributedStyle STYLE_ERROR = AttributedStyle.DEFAULT.foreground(128, 0, 0);
  public static final AttributedStyle STYLE_FAINT = AttributedStyle.DEFAULT.faint();
  public static final AttributedStyle STYLE_YELLOW =
      AttributedStyle.DEFAULT.foreground(128, 128, 0);
  public static final AttributedStyle STYLE_GREEN = AttributedStyle.DEFAULT.foreground(0, 128, 0);
  public static final AttributedStyle STYLE_BLUE = AttributedStyle.DEFAULT.foreground(0, 0, 128);

  private Integer exitWithCode;
  private NessieApiV2 nessieApi;
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
  }

  public Reference getCurrentReference() {
    if (nessieApi == null) {
      throw new NotConnectedException();
    }
    return currentReference;
  }

  public void connected(NessieApiV2 nessieApi) {
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
    this.nessieApi = nessieApi;
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

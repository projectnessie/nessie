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
package org.projectnessie.nessie.cli.jsonpretty;

import static org.jline.utils.AttributedStyle.CYAN;
import static org.jline.utils.AttributedStyle.GREEN;
import static org.jline.utils.AttributedStyle.YELLOW;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.PrintWriter;
import org.jline.terminal.Terminal;
import org.jline.utils.AttributedString;
import org.jline.utils.AttributedStyle;
import org.projectnessie.nessie.cli.jsongrammar.JsonCLexer;
import org.projectnessie.nessie.cli.jsongrammar.Token;

/** Minimalistic JSON pretty-print and highlighting. */
public class JsonPretty {
  private final Terminal terminal;
  private final PrintWriter writer;

  private static final AttributedStyle STYLE_BOOL =
      AttributedStyle.DEFAULT.bold().foreground(YELLOW);
  private static final AttributedStyle STYLE_NULL =
      AttributedStyle.DEFAULT.bold().foreground(YELLOW);
  private static final AttributedStyle STYLE_STRING =
      AttributedStyle.DEFAULT.bold().foreground(GREEN);
  private static final AttributedStyle STYLE_NUMBER = AttributedStyle.DEFAULT.foreground(CYAN);
  private static final AttributedStyle STYLE_SEPARATOR = AttributedStyle.DEFAULT;
  private static final AttributedStyle STYLE_BRACKET_BRACE = AttributedStyle.DEFAULT;

  private static final ObjectMapper MAPPER = new ObjectMapper();

  public JsonPretty(Terminal terminal, PrintWriter writer) {
    this.terminal = terminal;
    this.writer = writer;
  }

  public void prettyPrintObject(Object object) {
    String str = null;
    try {
      str = MAPPER.writeValueAsString(object);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
    prettyPrint(str);
  }

  public void prettyPrint(String jsonString) {
    int indent = 0;
    JsonCLexer lexer = new JsonCLexer(jsonString);
    for (Token t = null; ; ) {
      t = lexer.getNextToken(t);
      switch (t.getType()) {
        case EOF:
          {
            newLine(indent);
            return;
          }
        case DUMMY:
        case INVALID:
          break;
        //
        case OPEN_BRACKET:
        case OPEN_BRACE:
          print(t, STYLE_BRACKET_BRACE);
          indent++;
          newLine(indent);
          break;
        case CLOSE_BRACKET:
        case CLOSE_BRACE:
          indent--;
          newLine(indent);
          print(t, STYLE_BRACKET_BRACE);
          break;
        case COMMA:
          print(t, STYLE_SEPARATOR);
          newLine(indent);
          break;
        case COLON:
          print(t, STYLE_SEPARATOR);
          printSpace();
          break;
        case NULL:
          print(t, STYLE_NULL);
          break;
        case TRUE:
        case FALSE:
          print(t, STYLE_BOOL);
          break;
        case NUMBER:
          print(t, STYLE_NUMBER);
          break;
        case STRING_LITERAL:
          print(t, STYLE_STRING);
          break;
        //
        case SINGLE_LINE_COMMENT:
        case MULTI_LINE_COMMENT:
        case WHITESPACE:
          /* ignore */
          break;

        default:
          print(t, AttributedStyle.DEFAULT);
          break;
      }
    }
  }

  public void printSpace() {
    writer.print(' ');
  }

  public void newLine(int indent) {
    writer.println();
    writer.print("  ".repeat(indent));
  }

  public void print(Token token, AttributedStyle style) {
    writer.print(new AttributedString(token.getSource(), style).toAnsi(terminal));
  }
}

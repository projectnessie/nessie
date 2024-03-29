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

  private final AttributedStyle styleBool = AttributedStyle.DEFAULT.bold().foreground(128, 128, 0);
  private final AttributedStyle styleNull = AttributedStyle.DEFAULT.bold().foreground(128, 128, 0);
  private final AttributedStyle styleString = AttributedStyle.DEFAULT.bold().foreground(0, 128, 0);
  private final AttributedStyle styleNumber = AttributedStyle.DEFAULT.foreground(0, 128, 128);
  private final AttributedStyle styleSeparator = AttributedStyle.DEFAULT;
  private final AttributedStyle styleBracketBrace = AttributedStyle.DEFAULT;
  private final AttributedStyle styleComment = AttributedStyle.DEFAULT.foreground(0, 128, 0);

  public JsonPretty(Terminal terminal, PrintWriter writer) {
    this.terminal = terminal;
    this.writer = writer;
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
          print(t, styleBracketBrace);
          indent++;
          newLine(indent);
          break;
        case CLOSE_BRACKET:
        case CLOSE_BRACE:
          indent--;
          newLine(indent);
          print(t, styleBracketBrace);
          break;
        case COMMA:
          print(t, styleSeparator);
          newLine(indent);
          break;
        case COLON:
          print(t, styleSeparator);
          printSpace();
          break;
        case NULL:
          print(t, styleNull);
          break;
        case TRUE:
        case FALSE:
          print(t, styleBool);
          break;
        case NUMBER:
          print(t, styleNumber);
          break;
        case STRING_LITERAL:
          print(t, styleString);
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

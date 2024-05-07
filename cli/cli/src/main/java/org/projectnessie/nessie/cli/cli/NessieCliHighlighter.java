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

import static org.jline.utils.AttributedStyle.CYAN;
import static org.jline.utils.AttributedStyle.DEFAULT;
import static org.jline.utils.AttributedStyle.GREEN;
import static org.jline.utils.AttributedStyle.RED;
import static org.jline.utils.AttributedStyle.YELLOW;

import java.util.regex.Pattern;
import org.jline.reader.Highlighter;
import org.jline.reader.LineReader;
import org.jline.utils.AttributedString;
import org.jline.utils.AttributedStringBuilder;
import org.jline.utils.AttributedStyle;
import org.projectnessie.nessie.cli.grammar.NessieCliLexer;
import org.projectnessie.nessie.cli.grammar.NessieCliParser;
import org.projectnessie.nessie.cli.grammar.ParseException;
import org.projectnessie.nessie.cli.grammar.Token;
import org.projectnessie.nessie.cli.grammar.ast.Command;
import org.projectnessie.nessie.cli.grammar.ast.Keyword;
import org.projectnessie.nessie.cli.grammar.ast.Literal;
import org.projectnessie.nessie.cli.grammar.ast.ReferenceTypes;

class NessieCliHighlighter implements Highlighter {

  private static final AttributedStyle STYLE_COMMAND = DEFAULT.foreground(GREEN);
  private static final AttributedStyle STYLE_REF_TYPE = DEFAULT.foreground(CYAN);
  private static final AttributedStyle STYLE_KEYWORD = DEFAULT.foreground(YELLOW);
  private static final AttributedStyle STYLE_INVALID = DEFAULT.foreground(RED);
  private static final AttributedStyle STYLE_LITERAL = DEFAULT.bold().italic();
  private static final AttributedStyle STYLE_OTHER = DEFAULT;

  NessieCliHighlighter() {}

  @Override
  public AttributedString highlight(LineReader reader, String buffer) {
    AttributedStringBuilder sb = new AttributedStringBuilder();
    int bufferIndex = 0;

    NessieCliLexer lexer = new NessieCliLexer(buffer);
    NessieCliParser parser = new NessieCliParser(lexer);
    try {
      parser.SingleStatement();
    } catch (ParseException e) {
      // ignore
    }

    // Seek to beginning
    int index = 0;
    for (; ; index--) {
      Token t = parser.getToken(index);
      if (t == null) {
        index++;
        break;
      }
    }

    // Iterate through all tokens
    for (; ; index++) {
      Token t = parser.getToken(index);
      if (t.getType().isEOF()) {
        break;
      }

      if (t.getBeginOffset() == t.getEndOffset()) {
        continue;
      }

      if (bufferIndex < t.getBeginOffset()) {
        sb.append(buffer.substring(bufferIndex, t.getBeginOffset()), STYLE_OTHER);
      }

      String substring = buffer.substring(t.getBeginOffset(), t.getEndOffset());
      AttributedStyle style;
      if (t.isInvalid()) {
        style = STYLE_INVALID;
      } else if (t instanceof Command) {
        style = STYLE_COMMAND;
      } else if (t instanceof Keyword) {
        style = STYLE_KEYWORD;
      } else if (t instanceof ReferenceTypes) {
        style = STYLE_REF_TYPE;
      } else if (t instanceof Literal) {
        style = STYLE_LITERAL;
      } else {
        style = STYLE_OTHER;
      }
      sb.append(substring, style);
      bufferIndex = t.getEndOffset();
    }

    if (bufferIndex < buffer.length()) {
      sb.append(buffer.substring(bufferIndex), STYLE_COMMAND);
    }

    return sb.toAttributedString();
  }

  @Override
  public void setErrorPattern(Pattern errorPattern) {
    //    System.err.println("ERROR PATTERN " + errorPattern);
  }

  @Override
  public void setErrorIndex(int errorIndex) {
    //    System.err.println("ERROR INDEX " + errorIndex);
  }
}

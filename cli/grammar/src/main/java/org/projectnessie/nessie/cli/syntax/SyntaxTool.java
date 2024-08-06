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
package org.projectnessie.nessie.cli.syntax;

import static org.jline.utils.AttributedStyle.CYAN;
import static org.jline.utils.AttributedStyle.GREEN;
import static org.jline.utils.AttributedStyle.RED;
import static org.jline.utils.AttributedStyle.YELLOW;

import com.google.common.annotations.VisibleForTesting;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.congocc.core.BNFProduction;
import org.jline.utils.AttributedCharSequence;
import org.jline.utils.AttributedStringBuilder;
import org.jline.utils.AttributedStyle;
import org.projectnessie.nessie.cli.grammar.Token;

/**
 * Used to generate resource files for Nessie-CLI online help and include files for the
 * projectnessie.org site.
 */
public class SyntaxTool {
  public static void main(String[] args) throws Exception {
    Path outputDir = Paths.get(args[0]);

    Files.createDirectories(outputDir);

    Map<String, String> preprocessorSymbols = new HashMap<>();
    for (int i = 2; i < args.length; i++) {
      String arg = args[i];
      int eq = arg.indexOf('=');
      String k = arg.substring(0, eq);
      String v = arg.substring(eq + 1);
      preprocessorSymbols.put(k, v);
    }

    Syntax syntax = new Syntax(Paths.get(args[1]), preprocessorSymbols);

    Map<SyntaxPrinter.Type, AttributedStyle> styleMap = ansiStyleMap();

    for (Map.Entry<String, BNFProduction> productionEntry :
        syntax.getGrammar().getProductionTable().entrySet()) {
      String name = productionEntry.getKey();

      List<Token.TokenType> leadingTokens = leadingTokens(name);

      String ansi = syntaxAsAnsi(leadingTokens, syntax, productionEntry.getValue(), styleMap);
      String markdown = syntaxAsMarkdown(leadingTokens, syntax, productionEntry.getValue());
      String plain = syntaxAsPlain(leadingTokens, syntax, productionEntry.getValue());
      String refs = syntaxReferences(syntax, productionEntry.getValue());

      Files.writeString(outputDir.resolve(name + ".ansi.txt"), ansi);
      Files.writeString(outputDir.resolve(name + ".plain.txt"), plain);
      Files.writeString(outputDir.resolve(name + ".md"), markdown);
      Files.writeString(outputDir.resolve(name + ".refs"), refs);
    }
  }

  @SuppressWarnings("unchecked")
  @VisibleForTesting
  static List<Token.TokenType> leadingTokens(String name)
      throws ClassNotFoundException, IllegalAccessException {
    List<Token.TokenType> leadingTokens = List.of();
    Class<?> statementClass = Class.forName("org.projectnessie.nessie.cli.grammar.ast." + name);
    try {
      leadingTokens =
          (List<Token.TokenType>) statementClass.getDeclaredField("LEADING_TOKENS").get(null);
    } catch (NoSuchFieldException nsf) {
      // ignore
    }
    return leadingTokens;
  }

  static Map<SyntaxPrinter.Type, AttributedStyle> ansiStyleMap() {
    Map<SyntaxPrinter.Type, AttributedStyle> styleMap = new EnumMap<>(SyntaxPrinter.Type.class);
    AttributedStyle def = AttributedStyle.DEFAULT;
    AttributedStyle bold = def.bold();
    styleMap.put(SyntaxPrinter.Type.PRE, def);
    styleMap.put(SyntaxPrinter.Type.POST, def);
    styleMap.put(SyntaxPrinter.Type.SEP, def);
    styleMap.put(SyntaxPrinter.Type.IDENTIFIER, bold.foreground(YELLOW));
    styleMap.put(SyntaxPrinter.Type.TERMINAL, bold.foreground(CYAN));
    styleMap.put(SyntaxPrinter.Type.NON_TERMINAL, bold.foreground(GREEN));
    styleMap.put(SyntaxPrinter.Type.UNKNOWN, bold.foreground(RED));
    return styleMap;
  }

  static final String MARKDOWN_ESCAPES = "\\`*_{}[]<>#+-.!";

  static String syntaxAsMarkdown(
      List<Token.TokenType> leadingTokens, Syntax syntax, BNFProduction production) {
    StringBuilder sb = new StringBuilder();

    sb.append("> ");

    SyntaxPrinter printer =
        new SyntaxPrinter() {
          @Override
          public void newline(String indent) {
            sb.append("<br>\n  ").append(indent);
          }

          @Override
          public void space() {
            sb.append(' ');
          }

          @Override
          public void write(Type type, String str) {
            String pre = "";
            String post = "";
            switch (type) {
              case PRE:
              case POST:
              case SEP:
                break;
              case IDENTIFIER:
                pre = "**`";
                post = "`**";
                break;
              case TERMINAL:
                pre = post = "`";
                break;
              case NON_TERMINAL:
                pre = post = "**";
                break;
              case UNKNOWN:
                break;
              default:
                break;
            }
            sb.append(pre);
            for (int i = 0; i < str.length(); i++) {
              char c = str.charAt(i);
              if (MARKDOWN_ESCAPES.indexOf(c) != -1) {
                sb.append('\\');
              }
              sb.append(c);
            }
            sb.append(post);
          }
        };

    for (Token.TokenType leadingToken : leadingTokens) {
      printer.write(SyntaxPrinter.Type.TERMINAL, leadingToken.name());
      printer.space();
    }

    syntax.print(production, printer);

    return sb.toString();
  }

  static String syntaxAsAnsi(
      List<Token.TokenType> leadingTokens,
      Syntax syntax,
      BNFProduction production,
      Map<SyntaxPrinter.Type, AttributedStyle> styleMap) {
    AttributedStringBuilder sb = new AttributedStringBuilder();

    SyntaxPrinter printer =
        new SyntaxPrinter() {
          @Override
          public void newline(String indent) {
            sb.append('\n').append(indent);
          }

          @Override
          public void space() {
            sb.append(' ');
          }

          @Override
          public void write(Type type, String str) {
            sb.append(str, styleMap.get(type));
          }
        };

    for (Token.TokenType leadingToken : leadingTokens) {
      printer.write(SyntaxPrinter.Type.TERMINAL, leadingToken.name());
      printer.space();
    }

    syntax.print(production, printer);

    return sb.toAnsi(256, AttributedCharSequence.ForceMode.Force256Colors);
  }

  static String syntaxAsPlain(
      List<Token.TokenType> leadingTokens, Syntax syntax, BNFProduction production) {
    StringBuilder sb = new StringBuilder();

    SyntaxPrinter printer =
        new SyntaxPrinter() {
          @Override
          public void newline(String indent) {
            sb.append('\n').append(indent);
          }

          @Override
          public void space() {
            sb.append(' ');
          }

          @Override
          public void write(Type type, String str) {
            sb.append(str);
          }
        };

    for (Token.TokenType leadingToken : leadingTokens) {
      printer.write(SyntaxPrinter.Type.TERMINAL, leadingToken.name());
      printer.space();
    }

    syntax.print(production, printer);

    return sb.toString();
  }

  static String syntaxReferences(Syntax syntax, BNFProduction production) {
    StringBuilder sb = new StringBuilder();

    SyntaxPrinter printer =
        new SyntaxPrinter() {
          final Set<String> seen = new HashSet<>();

          @Override
          public void newline(String indent) {}

          @Override
          public void space() {}

          @Override
          public void write(Type type, String str) {
            if (type == Type.NON_TERMINAL) {
              if (seen.add(str)) {
                sb.append(str).append('\n');
              }
            }
          }
        };

    syntax.print(production, printer);

    return sb.toString();
  }
}

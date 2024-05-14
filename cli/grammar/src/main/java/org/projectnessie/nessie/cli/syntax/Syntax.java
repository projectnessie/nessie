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

import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.congocc.core.BNFProduction;
import org.congocc.core.Expansion;
import org.congocc.core.Grammar;
import org.congocc.core.LexerData;
import org.congocc.core.NonTerminal;
import org.congocc.core.RegularExpression;
import org.congocc.parser.Node;
import org.congocc.parser.Token;
import org.congocc.parser.tree.BaseNode;
import org.congocc.parser.tree.Delimiter;
import org.congocc.parser.tree.Identifier;
import org.congocc.parser.tree.Operator;
import org.congocc.parser.tree.RegexpStringLiteral;
import org.congocc.parser.tree.Terminal;
import org.congocc.parser.tree.TokenActivation;
import org.congocc.parser.tree.UP_TO_HERE;

public class Syntax {
  private final Grammar grammar;
  private final Map<String, String> stringLiterals;
  private final String level;

  public Syntax(Path grammarFile) throws IOException {
    this.grammar = new Grammar(null, "java", 17, false, Map.of());
    this.level = "    ";

    grammar.parse(grammarFile, true);

    grammar.doSanityChecks();
    grammar.generateLexer();

    LexerData lexerData = grammar.getLexerData();
    stringLiterals = new HashMap<>();
    for (RegularExpression orderedNamedToken : lexerData.getOrderedNamedTokens()) {
      if (orderedNamedToken instanceof RegexpStringLiteral) {
        RegexpStringLiteral regexpStringLiteral = (RegexpStringLiteral) orderedNamedToken;
        String literalString = regexpStringLiteral.getLiteralString();
        if (literalString != null) {
          stringLiterals.put(
              regexpStringLiteral.getLabel(), regexpStringLiteral.getLiteralString());
        }
      }
    }
  }

  public Grammar getGrammar() {
    return grammar;
  }

  private SyntaxPrinter syntaxPrinter;

  public void print(BNFProduction production, SyntaxPrinter syntaxPrinter) {
    reset();
    try {
      this.syntaxPrinter = syntaxPrinter;

      Node prod = production;

      // Skip leading 'Identifier : '
      for (int i = 0; i < production.size(); i++) {
        if (production.get(i) instanceof Operator
            && ((Operator) production.get(i)).getType() == Token.TokenType.COLON) {
          prod = new BaseNode();
          for (i++; i < production.size(); i++) {
            prod.add(production.get(i));
          }
          break;
        }
      }

      printNest(prod, stringLiterals, "", "\n", "", false);
    } finally {
      this.syntaxPrinter = null;
    }
  }

  private void print(Node node, Map<String, String> stringLiterals) {
    if (node instanceof Terminal) {
      Terminal terminal = (Terminal) node;
      String label = terminal.getLabel();
      write(SyntaxPrinter.Type.TERMINAL, stringLiterals.get(label));
    } else if (node instanceof NonTerminal) {
      NonTerminal nonTerminal = (NonTerminal) node;
      write(SyntaxPrinter.Type.NON_TERMINAL, nonTerminal.getName());
    } else if (node instanceof Identifier) {
      Identifier identifier = (Identifier) node;
      write(SyntaxPrinter.Type.IDENTIFIER, identifier.toString());
    } else if (node instanceof Expansion) {
      Expansion expansion = (Expansion) node;
      switch (expansion.getSimpleName()) {
        case "ZeroOrOne":
          printNest(node, stringLiterals, "[", "]", "", true);
          break;
        case "ZeroOrMore":
          printNest(node, stringLiterals, "{", "}", "", true);
          break;
        case "ExpansionWithParentheses":
        case "ExpansionSequence":
          printNest(node, stringLiterals, "", "", "", false);
          break;
        case "ExpansionChoice":
          printNest(node, stringLiterals, "", "", "|", true);
          break;
        default:
          break;
      }
    } else {
      write(SyntaxPrinter.Type.UNKNOWN, node.getClass().getSimpleName() + " : " + node.getType());
    }
  }

  private int currentIndent;
  private boolean requireNewline;
  private boolean hadContent;
  private boolean anythingPrinted;

  private void reset() {
    currentIndent = 0;
    requireNewline = false;
    hadContent = false;
    anythingPrinted = false;
  }

  private void write(SyntaxPrinter.Type type, String str) {
    if (str == null || str.isEmpty()) {
      return;
    }

    if (requireNewline) {
      if (anythingPrinted) {
        syntaxPrinter.newline(level.repeat(currentIndent));
        requireNewline = false;
      }
      hadContent = false;
    } else if (hadContent) {
      syntaxPrinter.space();
    }

    syntaxPrinter.write(type, str);
    hadContent = true;
    anythingPrinted = true;
  }

  private void printNest(
      Node node,
      Map<String, String> stringLiterals,
      String pre,
      String post,
      String sep,
      boolean addIndent) {
    List<Node> filtered = filterIgnored(node);
    if (filtered.isEmpty()) {
      return;
    }

    addIndent &= anythingPrinted;
    if (addIndent) {
      requireNewline = true;
    }

    write(SyntaxPrinter.Type.PRE, pre);

    if (addIndent) {
      currentIndent++;
    }

    boolean first = true;
    for (Node n : filtered) {
      if (first) {
        first = false;
      } else {
        write(SyntaxPrinter.Type.SEP, sep);
      }
      print(n, stringLiterals);
    }

    write(SyntaxPrinter.Type.POST, post);

    if (addIndent) {
      currentIndent--;
      requireNewline = true;
    }
  }

  private List<Node> filterIgnored(Node node) {
    return node.stream().filter(this::isNotIgnored).collect(Collectors.toList());
  }

  private boolean isNotIgnored(Node node) {
    if (node instanceof BNFProduction) {
      return false;
    } else if (node instanceof Operator) {
      return false;
    } else if (node instanceof Delimiter) {
      return false;
    } else if (node instanceof TokenActivation) {
      return false;
    } else if (node instanceof UP_TO_HERE) {
      return false;
    } else if (node instanceof Identifier) {
      return true;
    } else if (node instanceof Terminal) {
      Terminal terminal = (Terminal) node;
      String label = terminal.getLabel();
      return label != null && stringLiterals.get(label) != null;
    } else if (node instanceof NonTerminal) {
      return true;
    } else if (node instanceof Expansion) {
      Expansion expansion = (Expansion) node;
      switch (expansion.getSimpleName()) {
        case "ExpansionWithParentheses":
        case "ExpansionSequence":
        case "ExpansionChoice":
        case "ZeroOrOne":
        case "ZeroOrMore":
          return node.stream().anyMatch(this::isNotIgnored);
        default:
          return false;
      }
    }
    return true;
  }
}

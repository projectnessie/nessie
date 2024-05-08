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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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

public class Syntax {
  private final Grammar grammar;
  private final Map<String, String> stringLiterals;
  private final String level;

  /** Intentionally hard coded and short so that lines on the website do not break. */
  private static final int SPLIT_LINE_LENGTH = 60;

  public Syntax(Path grammarFile, Map<String, String> preprocessorSymbols) throws IOException {
    this.grammar = new Grammar(null, "java", 17, false, preprocessorSymbols);
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

  public void print(BNFProduction production, SyntaxPrinter syntaxPrinter) {
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

    Coll root = new Coll("", "");
    root.collectFrom(prod);
    root.maybeSplit();
    root.print(syntaxPrinter);
  }

  /*
   * The following is a full refactor of the first variant, which produced too many line breaks
   * and indention was not always correct or at least misleading.
   *
   * It basically first produces a sequence of `Obj` items (can be `Element` or `Coll`(ection)).
   * Collections are flattened as soon as possible.
   *
   * Then calculate the rendered length in "characters", if that's above the hard coded limit,
   * then break the first inner collection into multiple lines, keeping adjacent keywords together.
   */

  abstract static class Obj {
    abstract void print(SyntaxPrinter syntaxPrinter);

    abstract int length();
  }

  class Newline extends Obj {
    final Coll coll;
    final String indent;

    Newline(Coll coll, String indent) {
      this.coll = coll;
      this.indent = indent;
    }

    @Override
    int length() {
      throw new UnsupportedOperationException();
    }

    @Override
    void print(SyntaxPrinter syntaxPrinter) {
      syntaxPrinter.newline(indent);
      boolean needSpace = false;
      for (String s : coll.pre) {
        if (needSpace) {
          syntaxPrinter.space();
        }
        syntaxPrinter.write(SyntaxPrinter.Type.PRE, s);
        needSpace = true;
      }

      boolean didNewline = false;
      for (Obj obj : coll.children) {
        if (obj instanceof Element && ((Element) obj).type == SyntaxPrinter.Type.SEP) {
          didNewline = true;
          syntaxPrinter.newline(indent);
          needSpace = false;
        }
        if (needSpace) {
          syntaxPrinter.space();
        }
        obj.print(syntaxPrinter);
        needSpace = true;
      }

      if (didNewline) {
        syntaxPrinter.newline(indent);
        needSpace = false;
      }
      for (int i = coll.post.size() - 1; i >= 0; i--) {
        if (needSpace) {
          syntaxPrinter.space();
        }
        syntaxPrinter.write(SyntaxPrinter.Type.POST, coll.post.get(i));
        needSpace = true;
      }
    }

    @Override
    public String toString() {
      return "Newline{coll=" + coll + ", indent='" + indent + "'}";
    }
  }

  class Coll extends Obj {
    final List<Obj> children = new ArrayList<>();
    final List<String> pre = new ArrayList<>();
    final List<String> post = new ArrayList<>();

    Coll(String pre, String post) {
      if (!pre.isEmpty()) {
        this.pre.add(pre);
      }
      if (!post.isEmpty()) {
        this.post.add(post);
      }
    }

    @Override
    void print(SyntaxPrinter syntaxPrinter) {
      boolean needSpace = false;
      for (String s : this.pre) {
        if (needSpace) {
          syntaxPrinter.space();
        }
        syntaxPrinter.write(SyntaxPrinter.Type.PRE, s);
        needSpace = true;
      }
      for (Obj obj : children) {
        if (needSpace) {
          syntaxPrinter.space();
        }
        obj.print(syntaxPrinter);
        needSpace = true;
      }
      for (int i = this.post.size() - 1; i >= 0; i--) {
        syntaxPrinter.space();
        syntaxPrinter.write(SyntaxPrinter.Type.POST, this.post.get(i));
      }
    }

    @Override
    int length() {
      int l = 0;
      boolean needSpace = false;
      for (String s : this.pre) {
        if (needSpace) {
          l++;
        }
        l += s.length();
        needSpace = true;
      }
      for (Obj obj : children) {
        if (needSpace) {
          l++;
        }
        l += obj.length();
        needSpace = true;
      }
      for (int i = this.post.size() - 1; i >= 0; i--) {
        l++;
        l += this.post.get(i).length();
      }
      return l;
    }

    void maybeSplit() {
      int len = length();
      if (len < SPLIT_LINE_LENGTH) {
        return;
      }

      // Puts child `Coll` elements and sequences of `Element`s onto separate lines.
      List<Obj> prev = new ArrayList<>(children);
      children.clear();

      boolean hadNewline = false;
      Coll line = null;
      for (Obj obj : prev) {
        if (obj instanceof Coll) {
          if (line != null) {
            children.add(new Newline(line, level));
            line = null;
          }
          children.add(new Newline((Coll) obj, level));
          hadNewline = true;
        } else {
          if (hadNewline) {
            if (line == null) {
              line = new Coll("", "");
            }
            line.children.add(obj);
          } else {
            children.add(obj);
          }
        }
      }
      if (line != null) {
        children.add(new Newline(line, level));
      }
    }

    void collectFrom(Node prod) {
      for (Node node : prod.children()) {
        if (node instanceof Terminal) {
          Terminal terminal = (Terminal) node;
          String label = terminal.getLabel();
          maybeAdd(SyntaxPrinter.Type.TERMINAL, stringLiterals.get(label));
        } else if (node instanceof NonTerminal) {
          NonTerminal nonTerminal = (NonTerminal) node;
          maybeAdd(SyntaxPrinter.Type.NON_TERMINAL, nonTerminal.getName());
        } else if (node instanceof Identifier) {
          Identifier identifier = (Identifier) node;
          maybeAdd(SyntaxPrinter.Type.IDENTIFIER, identifier.toString());
        } else if (node instanceof Operator) {
          Operator operator = (Operator) node;
          String source = operator.getSource();
          if ("|".equals(source)) {
            children.add(new Element(SyntaxPrinter.Type.SEP, source));
          }
        } else if (node instanceof Delimiter) {
          // ignore
        } else if (node instanceof Expansion) {
          Expansion expansion = (Expansion) node;
          Coll coll = null;
          switch (expansion.getSimpleName()) {
            case "ZeroOrOne":
              coll = new Coll("[", "]");
              break;
            case "ZeroOrMore":
              coll = new Coll("{", "}");
              break;
            case "ExpansionWithParentheses":
              coll = new Coll("(", ")");
              break;
            case "ExpansionSequence":
            case "ExpansionChoice":
              coll = new Coll("", "");
              break;
            default:
              break;
          }
          if (coll != null) {
            coll.collectFrom(node);
            children.add(coll);
          }
        }
      }

      // inline all `Coll` elements that have no 'pre' + 'post'
      List<Obj> prev = new ArrayList<>(children);
      children.clear();
      for (Obj ch : prev) {
        if (ch instanceof Coll) {
          Coll coll = (Coll) ch;
          if (coll.noPrePost()) {
            children.addAll(coll.children);
          } else {
            children.add(ch);
          }
        } else {
          children.add(ch);
        }
      }

      // inline all directly nested `Coll` elements with 'size == 1' and combine 'pre' + 'post'
      while (children.size() == 1) {
        Obj ch = children.get(0);
        if (ch instanceof Coll) {
          Coll coll = (Coll) ch;
          if (coll.children.size() == 1) {
            this.pre.addAll(coll.pre);
            this.post.addAll(coll.post);
            this.children.clear();
            this.children.addAll(coll.children);
            continue;
          } else if (coll.noPrePost()) {
            this.children.clear();
            this.children.addAll(coll.children);
            continue;
          }
        }
        break;
      }
    }

    private boolean noPrePost() {
      return this.pre.isEmpty() && this.post.isEmpty();
    }

    void maybeAdd(SyntaxPrinter.Type type, String str) {
      if (str == null || str.isEmpty()) {
        return;
      }
      children.add(new Element(type, str));
    }

    @Override
    public String toString() {
      return "Coll{children=" + children + ", pre=" + pre + ", post=" + post + '}';
    }
  }

  static class Element extends Obj {
    final SyntaxPrinter.Type type;
    final String text;

    public Element(SyntaxPrinter.Type type, String text) {
      this.type = type;
      this.text = text;
    }

    @Override
    void print(SyntaxPrinter syntaxPrinter) {
      syntaxPrinter.write(type, text);
    }

    @Override
    int length() {
      return text.length();
    }

    @Override
    public String toString() {
      return "Element{type=" + type + ", text='" + text + '\'' + '}';
    }
  }
}

/*
 * Copyright (C) 2022 Dremio
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
package org.projectnessie.tools.contentgenerator.keygen;

import static java.util.Collections.unmodifiableList;
import static org.projectnessie.tools.contentgenerator.keygen.Functions.resolveFunction;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import org.projectnessie.tools.contentgenerator.keygen.KeyGenerator.Func;

class PatternParser {

  private final String pattern;
  private final ArrayList<Func> generators = new ArrayList<>();

  PatternParser(String pattern) {
    this.pattern = pattern;
  }

  enum State {
    CONST,
    DOLLAR,
    FUNC
  }

  IllegalArgumentException parseError(int i, String msg) {
    return new IllegalArgumentException(
        "Failure parsing pattern '" + pattern + "' at col #" + i + ": " + msg);
  }

  IllegalArgumentException parseError(int i, String msg, Exception e) {
    return new IllegalArgumentException(
        "Failure parsing pattern '" + pattern + "' at col #" + i + ": " + msg, e);
  }

  List<Func> parse() {
    StringBuilder currentConstant = new StringBuilder();
    StringBuilder currentFunc = new StringBuilder();
    String p = pattern;
    int l = p.length();
    State state = State.CONST;
    for (int i = 0; i < l; i++) {
      char c = p.charAt(i);

      switch (state) {
        case CONST:
          if (c == '$') {
            maybeAddConstant(currentConstant);
            state = State.DOLLAR;
          } else {
            currentConstant.append(c);
          }
          break;
        case DOLLAR:
          if (c == '{') {
            state = State.FUNC;
          } else if (c == '$') {
            currentConstant.append('$');
            state = State.CONST;
          } else {
            throw parseError(i, "illegal function declaration");
          }
          break;
        case FUNC:
          if (c == '}') {
            try {
              parseFuncPattern(currentFunc);
              state = State.CONST;
            } catch (Exception e) {
              throw parseError(i, "failure parsing function declaration: " + e.getMessage(), e);
            }
          } else {
            currentFunc.append(c);
          }
          break;
        default:
          break;
      }
    }

    switch (state) {
      case DOLLAR:
      case FUNC:
        throw parseError(l, "unclosed function pattern at the end of the pattern");
      default:
        break;
    }

    maybeAddConstant(currentConstant);

    generators.trimToSize();
    return unmodifiableList(generators);
  }

  private void parseFuncPattern(StringBuilder currentFunc) {
    parseFuncPattern(currentFunc.toString());
    currentFunc.setLength(0);
  }

  private void parseFuncPattern(String func) {
    func = func.trim();

    int i = func.indexOf(',');
    String name;
    List<String> args = new ArrayList<>();
    if (i == -1) {
      name = func;
    } else {
      name = func.substring(0, i).trim();
      while (true) {
        i++;
        if (i >= func.length()) {
          break;
        }

        int next = func.indexOf(',', i);
        if (next == -1) {
          args.add(func.substring(i).trim());
          break;
        }
        args.add(func.substring(i, next).trim());
        i = next;
      }
    }

    Queue<String> params = new ArrayDeque<>(args);
    resolveFunction(name).generate(params).ifPresent(generators::add);
  }

  private void maybeAddConstant(StringBuilder currentConstant) {
    if (currentConstant.length() > 0) {
      String c = currentConstant.toString();
      currentConstant.setLength(0);
      generators.add((r, sb) -> sb.append(c));
    }
  }
}

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
package org.projectnessie.nessie.cli.completer;

import static java.util.Locale.ROOT;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import org.projectnessie.nessie.cli.grammar.CompletionType;
import org.projectnessie.nessie.cli.grammar.NessieCliParser;
import org.projectnessie.nessie.cli.grammar.ParseException;
import org.projectnessie.nessie.cli.grammar.Token;
import org.projectnessie.nessie.cli.grammar.Token.TokenType;

public abstract class CliCompleter {
  private final String source;
  private final NessieCliParser parser;

  private final Consumer<NessieCliParser> producer;

  public CliCompleter(
      String input,
      int cursor,
      Function<String, NessieCliParser> parserForSource,
      Consumer<NessieCliParser> producer) {
    this.source = input.substring(0, cursor);

    this.parser = parserForSource.apply(source);

    this.producer = producer;
  }

  public NessieCliParser parser() {
    return parser;
  }

  /**
   * Runs a completion attempt.
   *
   * <p>Provides completions for tokens via {@link #tokenCandidateStartsWith(String, TokenType)} et
   * al and calls {@link #completeWithLiteral(CompletionType, String, String, boolean)} for
   * literals.
   *
   * @return {@code true} if the input could not be parsed, {@code false} if the input could be
   *     successfully parsed.
   */
  public boolean tryStatement() {
    try {
      producer.accept(parser);

      // Input was successfully parsed. If the last token is an identifier, try to complete it.

      if (parser.getToken(0).getType().isEOF()) {
        Token prev = parser.getToken(-1);
        switch (prev.getType()) {
          case IDENTIFIER:
          case STRING_LITERAL:
          case URI:
            completeWithIdentifier(
                source.substring(0, prev.getEndOffset()), source.substring(prev.getBeginOffset()));
            break;
          default:
            // cannot have "unfinished" keywords for a successfully parsed statement
            break;
        }
      }

      List<TokenType> optionalTokens = parser.optionalNextTokenTypes();
      if (!optionalTokens.isEmpty()) {
        String preceding = source;
        if (!preceding.isEmpty()
            && !Character.isWhitespace(preceding.charAt(preceding.length() - 1))) {
          preceding += " ";
        }
        for (TokenType optionalToken : optionalTokens) {
          if (parser.isTokenActive(optionalToken)) {
            tokenCandidateOther(preceding, optionalToken);
          }
        }
      }

      return false;
    } catch (ParseException e) {
      int index = 0;
      Token currentToken = parser.getToken(index);

      // Go to the token on which the cursor is.
      int cursor = source.length();
      while (true) {
        if (currentToken.getBeginOffset() <= cursor && cursor <= currentToken.getEndOffset()) {
          break;
        }
        if (cursor < currentToken.getEndOffset()) {
          Token prev = parser.getToken(--index);
          if (prev == null) {
            break;
          }
          currentToken = prev;
        }
        if (cursor >= currentToken.getEndOffset()) {
          Token next = parser.getToken(++index);
          if (next == null || next.getType().isEOF()) {
            break;
          }
          currentToken = next;
        }
      }

      // Go back to the first invalid token. This finds the start of incomplete string literals,
      // for example for an input like 'CREATE BRANCH "foo bar', it sets `currentToken` to the
      // invalid token '"foo'. Have to skip over all non-INVALID tokens.
      for (int i = index; ; i--) {
        Token c = parser.getToken(i);
        if (c == null) {
          break;
        } else if (c.getType().isInvalid()) {
          currentToken = c;
        }
      }

      String trimmed;
      if (currentToken.isInvalid()) {
        trimmed = source.substring(0, currentToken.getBeginOffset());
      } else {
        trimmed = source;
        if (!trimmed.isEmpty() && !Character.isWhitespace(trimmed.charAt(trimmed.length() - 1))) {
          trimmed += ' ';
        }
      }

      String currentSource = source.substring(currentToken.getBeginOffset(), cursor);
      String currentUpper = currentSource.toUpperCase(ROOT);
      boolean currentIsEmpty = currentSource.trim().isEmpty();

      boolean handledIdentifier = false;

      List<TokenType> expectedTokenTypes = new ArrayList<>(parser.optionalNextTokenTypes());
      for (TokenType expected : e.getExpectedTokenTypes()) {
        switch (expected) {
          case EOF:
          case DUMMY:
          case WHITESPACE:
            break;
          default:
            if (parser.isTokenActive(expected)) {
              expectedTokenTypes.add(expected);
            }
        }
      }

      for (TokenType expected : expectedTokenTypes) {
        switch (expected) {
          case IDENTIFIER:
          case STRING_LITERAL:
          case URI:
            // Don't call 'completeWithIdentifier()' twice for the same input.
            if (!handledIdentifier) {
              completeWithIdentifier(trimmed, currentSource);
              handledIdentifier = true;
            }
            break;

          default:
            // Replace the underscore with a space for "multi-word" tokens.
            String tokenString = parser.tokenToLiteral(expected);
            String tokenUpper = tokenString.toUpperCase(ROOT);

            if (currentIsEmpty) {
              tokenCandidateOther(trimmed, expected);
            } else if (tokenUpper.startsWith(currentUpper)) {
              tokenCandidateStartsWith(trimmed, expected);
            } else if (tokenUpper.contains(currentUpper)) {
              tokenCandidateContains(trimmed, expected);
            } else {
              tokenCandidateOther(trimmed, expected);
            }
            break;
        }
      }
      return true;
    }
  }

  private void completeWithIdentifier(String preceding, String toComplete) {
    boolean quoted = false;
    char c = !toComplete.isEmpty() ? toComplete.charAt(0) : 0;
    if (c == '\"' || c == '\'' || c == '`') {
      // in STRING_LITERAL
      quoted = true;
      toComplete = toComplete.substring(1);
      if (toComplete.endsWith("" + c)) {
        toComplete = toComplete.substring(0, toComplete.length() - 1);
      }
    }

    completeWithLiteral(parser.completionType(), preceding, toComplete, quoted);
  }

  protected abstract void completeWithLiteral(
      CompletionType completionType, String preceding, String toComplete, boolean quoted);

  protected abstract void tokenCandidateStartsWith(String preceding, TokenType toComplete);

  protected abstract void tokenCandidateContains(String preceding, TokenType toComplete);

  protected abstract void tokenCandidateOther(String preceding, TokenType toComplete);
}

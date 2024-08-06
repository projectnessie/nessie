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

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.jline.reader.Candidate;
import org.jline.reader.LineReader;
import org.jline.reader.ParsedLine;
import org.jline.reader.impl.completer.SystemCompleter;
import org.projectnessie.client.NessieConfigConstants;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Reference;
import org.projectnessie.nessie.cli.completer.CliCompleter;
import org.projectnessie.nessie.cli.grammar.CompletionType;
import org.projectnessie.nessie.cli.grammar.NessieCliParser;
import org.projectnessie.nessie.cli.grammar.Token.TokenType;

class NessieCliCompleter extends SystemCompleter {

  private static final List<String> NESSIE_CONFIG_KEYS =
      Arrays.stream(NessieConfigConstants.class.getDeclaredFields())
          .filter(f -> f.getName().startsWith("CONF_"))
          .map(
              f -> {
                try {
                  return f.get(null);
                } catch (IllegalAccessException e) {
                  throw new RuntimeException(e);
                }
              })
          .map(String.class::cast)
          .filter(k -> k.startsWith("nessie."))
          .sorted()
          .collect(Collectors.toList());

  private final BaseNessieCli nessieCli;

  NessieCliCompleter(BaseNessieCli nessieCli) {
    this.nessieCli = nessieCli;
  }

  @Override
  public void complete(LineReader reader, ParsedLine line, List<Candidate> candidates) {
    new CliCompleter(
        line.line(),
        line.cursor(),
        nessieCli::newParserForSource,
        NessieCliParser::SingleStatement) {
      @Override
      protected void completeWithLiteral(
          CompletionType completionType, String preceding, String toComplete, boolean quoted) {
        switch (completionType) {
          case REFERENCE_NAME:
            AtomicInteger counter = new AtomicInteger();
            // TODO providing all (think: too many) reference names can "overwhelm" completion.
            //  JLine then warns with something like 'JLine terminal: do you wish to see all 218
            //  possibilities (14 lines)?', which is not nice and does not help.
            //  The implementation should not return "too many" options but enough (starting
            //  strings) to continue completion, so users do not have to see that warning.
            // TODO also pass 'toComplete', if not empty, as a filter down to Nessie
            // TODO also pass the reference type, if given, as a filter down to Nessie (need to know
            //  the statement parsed up to here :( )
            nessieCli
                .nessieApi()
                .ifPresent(
                    api -> {
                      try {
                        api.getAllReferences().stream()
                            .map(Reference::getName)
                            .forEach(refName -> candidate(refName, counter.getAndIncrement()));
                      } catch (NessieNotFoundException e) {
                        throw new RuntimeException(e);
                      }
                    });
            break;
          case CONNECT_OPTIONS:
            for (int i = 0; i < NESSIE_CONFIG_KEYS.size(); i++) {
              candidate("\"" + NESSIE_CONFIG_KEYS.get(i) + "\"", i);
            }
            break;
          case CONTENT_KEY:
            // TODO do something ?
            break;
          default:
            // do nothing - don't break anything
            break;
        }
      }

      @Override
      protected void tokenCandidateStartsWith(String preceding, TokenType toComplete) {
        candidate(toComplete, 1_000);
      }

      @Override
      protected void tokenCandidateContains(String preceding, TokenType toComplete) {
        candidate(toComplete, 2_000);
      }

      @Override
      protected void tokenCandidateOther(String preceding, TokenType toComplete) {
        candidate(toComplete, 3_000);
      }

      private void candidate(TokenType token, int offset) {
        candidate(parser().tokenToLiteral(token), offset);
      }

      private void candidate(String tokenString, int offset) {
        candidates.add(
            new Candidate(
                tokenString,
                tokenString,
                null,
                null,
                null,
                null,
                true,
                offset + candidates.size()));
      }
    }.tryStatement();
  }
}

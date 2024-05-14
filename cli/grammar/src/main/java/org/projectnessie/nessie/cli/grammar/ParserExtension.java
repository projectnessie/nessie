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
package org.projectnessie.nessie.cli.grammar;

import java.util.List;
import org.projectnessie.nessie.cli.grammar.Token.TokenType;

public interface ParserExtension {

  /** Retrieves the user-facing (lexer) representation of a token, useable for auto-completion. */
  String tokenToLiteral(TokenType tokenType);

  /**
   * Retrieves list of <em>optional</em> tokens that are acceptable at the current "parsing
   * position".
   *
   * <p>The parsing code updates the list of optional token types for optional blocks.
   */
  List<TokenType> optionalNextTokenTypes();

  /**
   * Returns the completion type for string literals / identifiers, which is meant for
   * auto-completion to suggest values like reference names, content keys and the like.
   */
  CompletionType completionType();
}

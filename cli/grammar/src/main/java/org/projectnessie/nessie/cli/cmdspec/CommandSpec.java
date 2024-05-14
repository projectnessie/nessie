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
package org.projectnessie.nessie.cli.cmdspec;

import jakarta.annotation.Nullable;
import org.projectnessie.nessie.cli.grammar.IdentifierOrLiteral;
import org.projectnessie.nessie.cli.grammar.Node;
import org.projectnessie.nessie.cli.grammar.ast.BaseNode;

public interface CommandSpec {
  CommandType commandType();

  @Nullable
  default Node sourceNode() {
    if (this instanceof Node) {
      return (Node) this;
    }
    return null;
  }

  default String stringValueOf(String child) {
    return IdentifierOrLiteral.stringValueOf((BaseNode) this, child);
  }
}

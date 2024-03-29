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
package org.projectnessie.nessie.cli.commands;

import jakarta.annotation.Nonnull;
import java.util.List;
import org.projectnessie.nessie.cli.cli.BaseNessieCli;
import org.projectnessie.nessie.cli.cmdspec.ExitCommandSpec;
import org.projectnessie.nessie.cli.grammar.Node;
import org.projectnessie.nessie.cli.grammar.Token;

public class ExitCommand extends NessieCommand<ExitCommandSpec> {
  public ExitCommand() {}

  @Override
  public void execute(@Nonnull BaseNessieCli cli, ExitCommandSpec spec) {
    cli.exitRepl(0);
  }

  public String name() {
    return Token.TokenType.EXIT.name();
  }

  public String description() {
    return "Exit the Nessie REPL.";
  }

  @Override
  public List<List<Node.NodeType>> matchesNodeTypes() {
    return List.of(List.of(Token.TokenType.EXIT));
  }
}

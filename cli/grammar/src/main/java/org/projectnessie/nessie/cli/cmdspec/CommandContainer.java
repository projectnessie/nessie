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

import java.util.ArrayList;
import java.util.List;
import org.projectnessie.nessie.cli.grammar.Node;
import org.projectnessie.nessie.cli.grammar.Token;
import org.projectnessie.nessie.cli.grammar.ast.AlterStatement;
import org.projectnessie.nessie.cli.grammar.ast.CreateStatement;
import org.projectnessie.nessie.cli.grammar.ast.DropStatement;
import org.projectnessie.nessie.cli.grammar.ast.Keyword;
import org.projectnessie.nessie.cli.grammar.ast.ListStatement;
import org.projectnessie.nessie.cli.grammar.ast.ShowStatement;

public interface CommandContainer extends Node {
  default List<CommandSpec> commandSpecs() {
    List<CommandSpec> commandSpecs = new ArrayList<>();
    for (Node child : children()) {

      if (child instanceof CommandSpec) {
        // Have a "proper" CommandSpec instance, use it.
        commandSpecs.add((CommandSpec) child);
        continue;
      }

      // Handle commands that may have no arguments. Commands w/o arguments (e.g. "SHOW LOG;") do
      // *not* produce a ShowLogCommandSpec, so we have to check the tokens.
      if (child instanceof ShowStatement) {
        CommandSpec spec = child.firstChildOfType(CommandSpec.class);
        if (spec == null) {
          Keyword kw = child.firstChildOfType(Keyword.class);
          switch (kw.getType()) {
            case LOG:
              spec = ImmutableShowLogCommandSpec.builder().sourceNode(child).build();
              break;
            case TABLE:
            case VIEW:
            case NAMESPACE:
              spec = ImmutableShowContentCommandSpec.builder().sourceNode(child).build();
              break;
            case REFERENCE:
              spec = ImmutableShowReferenceCommandSpec.builder().sourceNode(child).build();
              break;
            default:
              // spec = null;
              break;
          }
        }
        if (spec != null) {
          commandSpecs.add(spec);
        }
        continue;
      } else if (child instanceof ListStatement) {
        CommandSpec spec = child.firstChildOfType(CommandSpec.class);
        if (spec == null) {
          Keyword kw = child.firstChildOfType(Keyword.class);
          switch (kw.getType()) {
            case CONTENTS:
              spec = ImmutableListContentsCommandSpec.builder().sourceNode(child).build();
              break;
            case REFERENCES:
              spec = ImmutableListReferencesCommandSpec.builder().sourceNode(child).build();
              break;
            default:
              // spec = null;
              break;
          }
        }
        if (spec != null) {
          commandSpecs.add(spec);
        }
        continue;
      } else if (child instanceof CreateStatement) {
        CommandSpec spec = child.firstChildOfType(CommandSpec.class);
        if (spec == null) {
          Keyword kw = child.firstChildOfType(Keyword.class);
          switch (kw.getType()) {
            case BRANCH:
            case TAG:
              spec = ImmutableCreateReferenceCommandSpec.builder().sourceNode(child).build();
              break;
            case NAMESPACE:
              spec = ImmutableCreateNamespaceCommandSpec.builder().sourceNode(child).build();
              break;
            default:
              // spec = null;
              break;
          }
        }
        if (spec != null) {
          commandSpecs.add(spec);
        }
        continue;
      } else if (child instanceof AlterStatement) {
        CommandSpec spec = child.firstChildOfType(CommandSpec.class);
        if (spec == null) {
          Keyword kw = child.firstChildOfType(Keyword.class);
          switch (kw.getType()) {
            case ALTER:
              spec = ImmutableAlterNamespaceCommandSpec.builder().sourceNode(child).build();
              break;
            default:
              // spec = null;
              break;
          }
        }
        if (spec != null) {
          commandSpecs.add(spec);
        }
        continue;
      } else if (child instanceof DropStatement) {
        CommandSpec spec = child.firstChildOfType(CommandSpec.class);
        if (spec == null) {
          Keyword kw = child.firstChildOfType(Keyword.class);
          switch (kw.getType()) {
            case BRANCH:
            case TAG:
            case NAMESPACE:
              spec = ImmutableDropReferenceCommandSpec.builder().sourceNode(child).build();
              break;
            default:
              // spec = null;
              break;
          }
        }
        if (spec != null) {
          commandSpecs.add(spec);
        }
        continue;
      }

      // Leaves us with the "simple no arg" commands HELP and EXIT
      if (child instanceof Token) {
        Token t = (Token) child;
        CommandSpec spec;
        switch (t.getType()) {
          case EXIT:
            spec = ImmutableExitCommandSpec.builder().sourceNode(child).build();
            break;
          case HELP:
            spec = ImmutableHelpCommandSpec.builder().sourceNode(child).build();
            break;
          case LIST:
            spec = ImmutableListReferencesCommandSpec.builder().sourceNode(child).build();
            break;
          default:
            spec = null;
            break;
        }
        if (spec != null) {
          commandSpecs.add(spec);
        }
        // continue;
      }
    }
    return commandSpecs;
  }
}

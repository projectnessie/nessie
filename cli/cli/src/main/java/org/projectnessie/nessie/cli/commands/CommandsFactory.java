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

import java.util.EnumMap;
import java.util.function.Supplier;
import org.projectnessie.nessie.cli.cmdspec.CommandSpec;
import org.projectnessie.nessie.cli.cmdspec.CommandType;

public class CommandsFactory {
  private static final EnumMap<CommandType, Supplier<NessieCommand<?>>> COMMAND_FACTORIES;

  static {
    COMMAND_FACTORIES = new EnumMap<>(CommandType.class);
    COMMAND_FACTORIES.put(CommandType.CONNECT, ConnectCommand::new);
    COMMAND_FACTORIES.put(CommandType.ASSIGN_REFERENCE, AssignReferenceCommand::new);
    COMMAND_FACTORIES.put(CommandType.CREATE_REFERENCE, CreateReferenceCommand::new);
    COMMAND_FACTORIES.put(CommandType.DROP_REFERENCE, DropReferenceCommand::new);
    COMMAND_FACTORIES.put(CommandType.CREATE_NAMESPACE, CreateNamespaceCommand::new);
    COMMAND_FACTORIES.put(CommandType.ALTER_NAMESPACE, AlterNamespaceCommand::new);
    COMMAND_FACTORIES.put(CommandType.DROP_CONTENT, DropContentCommand::new);
    COMMAND_FACTORIES.put(CommandType.EXIT, ExitCommand::new);
    COMMAND_FACTORIES.put(CommandType.HELP, HelpCommand::new);
    COMMAND_FACTORIES.put(CommandType.LIST_CONTENTS, ListContentsCommand::new);
    COMMAND_FACTORIES.put(CommandType.LIST_REFERENCES, ListReferencesCommand::new);
    COMMAND_FACTORIES.put(CommandType.MERGE_BRANCH, MergeBranchCommand::new);
    COMMAND_FACTORIES.put(CommandType.SHOW_LOG, ShowLogCommand::new);
    COMMAND_FACTORIES.put(CommandType.SHOW_CONTENT, ShowContentCommand::new);
    COMMAND_FACTORIES.put(CommandType.SHOW_REFERENCE, ShowReferenceCommand::new);
    COMMAND_FACTORIES.put(CommandType.USE_REFERENCE, UseReferenceCommand::new);
    COMMAND_FACTORIES.put(CommandType.REVERT_CONTENT, RevertContentCommand::new);
  }

  @SuppressWarnings({"rawtypes", "unchecked", "UnnecessaryLocalVariable"})
  public static <SPEC extends CommandSpec> NessieCommand<SPEC> buildCommandInstance(
      CommandType commandType) {
    NessieCommand cmd = COMMAND_FACTORIES.get(commandType).get();
    return cmd;
  }
}

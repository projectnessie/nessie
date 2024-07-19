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

public enum CommandType {
  CONNECT("ConnectStatement"),
  CREATE_REFERENCE("CreateReferenceStatement"),
  CREATE_NAMESPACE("CreateNamespaceStatement"),
  DROP_REFERENCE("DropReferenceStatement"),
  DROP_CONTENT("DropContentStatement"),
  ASSIGN_REFERENCE("AssignReferenceStatement"),
  ALTER_NAMESPACE("AlterNamespaceStatement"),
  EXIT("ExitStatement"),
  HELP("HelpStatement"),
  LIST_CONTENTS("ListContentsStatement"),
  LIST_REFERENCES("ListReferencesStatement"),
  MERGE_BRANCH("MergeBranchStatement"),
  REVERT_CONTENT("RevertContentStatement"),
  SHOW_LOG("ShowLogStatement"),
  SHOW_CONTENT("ShowContentStatement"),
  SHOW_REFERENCE("ShowReferenceStatement"),
  USE_REFERENCE("UseReferenceStatement"),
  ;

  private final String statementName;

  CommandType(String statementName) {
    this.statementName = statementName;
  }

  public String statementName() {
    return statementName;
  }
}

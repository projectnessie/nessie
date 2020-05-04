/*
 * Copyright (C) 2020 Dremio
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
package com.dremio.iceberg.server.jgit;

import com.dremio.iceberg.model.Branch;
import com.dremio.iceberg.model.BranchTable;
import java.io.IOException;
import java.security.Principal;
import java.util.List;
import java.util.Map;

public interface JGitContainer {

  @SuppressWarnings("VariableDeclarationUsageDistance")
  void update(String branch, Map<String, Map<String, String>> tables) throws Throwable;

  void read(String branch, String table) throws Throwable;

  void create(String branch) throws IOException;

  List<Branch> getBranches() throws IOException;

  Branch getBranch(String branch) throws IOException;

  BranchTable getTable(String branch, String tableName) throws IOException;

  void commit(String branch,
              String tableName,
              BranchTable table,
              Principal userPrincipal) throws IOException;
}

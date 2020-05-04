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
import com.dremio.iceberg.model.VersionedWrapper;
import java.io.IOException;
import java.security.Principal;
import java.util.List;

public interface JGitContainer {

  void create(String branch, String baseBranch, Principal principal) throws IOException;

  List<Branch> getBranches() throws IOException;

  VersionedWrapper<Branch> getBranch(String branch) throws IOException;

  VersionedWrapper<BranchTable> getTable(String branch, String tableName) throws IOException;

  void commit(String branch,
              Principal userPrincipal,
              Long version,
              BranchTable... tables) throws IOException;

}

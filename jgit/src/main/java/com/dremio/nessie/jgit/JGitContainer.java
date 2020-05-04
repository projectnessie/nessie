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

package com.dremio.nessie.jgit;

import com.dremio.nessie.model.Branch;
import com.dremio.nessie.model.BranchTable;
import com.dremio.nessie.model.HeadVersionPair;
import com.dremio.nessie.model.VersionedStringWrapper;
import java.io.IOException;
import java.security.Principal;
import java.util.List;

public interface JGitContainer {

  VersionedStringWrapper<Branch> create(String branch, String baseBranch, Principal principal)
      throws IOException;

  List<Branch> getBranches() throws IOException;

  VersionedStringWrapper<Branch> getBranch(String branch) throws IOException;

  VersionedStringWrapper<BranchTable> getTable(String branch, String tableName) throws IOException;

  VersionedStringWrapper<BranchTable> commit(String branch,
                                             Principal userPrincipal,
                                             HeadVersionPair version,
                                             BranchTable... tables) throws IOException;

  void deleteBranch(String branch,
                    HeadVersionPair headVersion,
                    boolean purge) throws IOException;

  List<String> getTables(String branch, String namespace) throws IOException;

  HeadVersionPair promote(String branch,
                          String mergeBranch,
                          HeadVersionPair version) throws IOException;
}

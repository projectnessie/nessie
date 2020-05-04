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

package com.dremio.nessie.backend;

import com.dremio.nessie.model.Branch;
import com.dremio.nessie.model.CommitMeta;
import com.dremio.nessie.model.Table;
import java.io.IOException;
import java.util.List;

public interface BranchController {

  List<Branch> getBranches() throws IOException;

  List<String> getTables(String branch, String namespace) throws IOException;

  Branch getBranch(String branch) throws IOException;

  Table getTable(String branch, String tableName, boolean metadata) throws IOException;

  Branch create(String branch, String baseBranch, CommitMeta commitMeta) throws IOException;

  String commit(String branch,
                CommitMeta commitMeta,
                String version,
                Table... tables) throws IOException;

  void deleteBranch(String branch,
                    String headVersion,
                    boolean purge,
                    CommitMeta meta) throws IOException;

  String promote(String branch,
                 String mergeBranch,
                 String version,
                 CommitMeta commitMeta,
                 boolean force,
                 boolean cherryPick,
                 String namespace) throws IOException;
}

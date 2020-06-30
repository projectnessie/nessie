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

package com.dremio.nessie.client;

import com.dremio.nessie.model.Branch;
import com.dremio.nessie.model.NessieConfiguration;
import com.dremio.nessie.model.Table;

public interface BaseClient extends AutoCloseable {

  /**
   * Fetch configuration from the server.
   */
  NessieConfiguration getConfig();

  /**
   * Get all known branches.
   */
  Branch[] getBranches();

  /**
   * Get a single table specific to a given branch.
   */
  Table getTable(String branch, String name, String namespace);

  /**
   * Get a branch for a given name.
   */
  Branch getBranch(String branchName);

  /**
   * Create a new branch. Branch name is the branch name and id is the branch to copy from.
   */
  Branch createBranch(Branch branch);

  /**
   * Commit a set of tables on a given branch.
   *
   * <p>
   * These could be updates, creates or deletes given the state of the backend and the tables being
   * commited. This could throw an exception if the version is incorrect. This implies that the
   * branch you are on is not up to date and there is a merge conflict.
   * </p>
   *
   * @param branch The branch to commit on. Its id is the commit version to commit on top of
   * @param tables list of tables to be added, deleted or modified
   */
  void commit(Branch branch, Table... tables);

  /**
   * Return a list of all table names for a branch.
   *
   * <p>
   * We do not return all table objects as its a costly operation. Only table names. Optionally
   * filtered by namespace
   * </p>
   */
  String[] getAllTables(String branch, String namespace);

  /**
   * Merge all commits from updateBranch onto branch. Optionally forcing.
   */
  void mergeBranch(Branch branch,
                   String updateBranch,
                   boolean force);

  /**
   * Delete a branch. Note this is potentially damaging if the branch is not fully merged.
   */
  void deleteBranch(Branch branch);
}

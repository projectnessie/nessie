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

import java.io.IOException;
import java.util.List;

import com.dremio.nessie.model.Branch;
import com.dremio.nessie.model.CommitMeta;
import com.dremio.nessie.model.Table;

/**
 * Branching and merging controller. Used by the server to maintain a consistent view of the
 * entire database and its various branches/states.
 *
 * <p>
 *   This provides a git-like interface for interacting with tables and branches in Nessie.
 *   The implementer is expected to provide CRUD-like operations for both Branches and Tables
 *   as well as be able to promote/merge/force-merge/cherry-pick tables from branch to another.
 * </p>
 */
public interface BranchController {

  /**
   * Show all known branches.
   *
   * <p>
   *   Akin to git show-ref.
   * </p>
   */
  List<Branch> getBranches() throws IOException;

  /**
   * List the table names for a specific branch (ref). Optionally filter by namespace.
   *
   * @param branch Branch or Ref to list from.
   * @param namespace optional namespace for namespaced databases
   * @return List of table ids
   */
  List<String> getTables(String branch, String namespace) throws IOException;

  /**
   * Retrieve a single branch/ref based on name.
   *
   * @param branch branch name
   */
  Branch getBranch(String branch) throws IOException;

  /**
   * Retrieve a table for a given branch. Optionally include any metadata available.
   *
   * @param branch branch being referenced
   * @param tableName name/id of table to retrieve
   * @param metadata flag to include table metadata
   */
  Table getTable(String branch, String tableName, boolean metadata) throws IOException;

  /**
   * Create a branch. Optionally inherit from baseBranch.
   *
   * <p>
   *   If baseBranch is null the branch will be created empty. If a known branch/ref the newly
   *   created branch will be identical to baseBranch at time of creation.
   * </p>
   *
   * @param branch name of new Branch
   * @param baseBranch branch to clone from. Optional
   * @param commitMeta metadata to log purpose of change and owner of change.
   */
  Branch create(String branch, String baseBranch, CommitMeta commitMeta) throws IOException;

  /**
   * Make changes to tables. This will update branch atomically to incorporate all changes.
   *
   * <p>
   *   The array of tables includes all changes to be made in this atomic transaction. The tables
   *   can be created, updates or deleted (via the isDeleted boolean). The version flag indicates
   *   which version of branch the client is aware of. This may be different from what the server
   *   knows about in a multi-tenant environment. The version string is used to provide consistency
   *   across clients via optimistic locking. If the server version is ahead of the client version
   *   the server will make best efforts to perform the commit anyways. The commit will fail if
   *   the client is trying to change a table that has been changed since 'version'.
   * </p>
   *
   * @param branch branch to make the changes on
   * @param commitMeta metadata to record owner and purpose of change
   * @param version provided version for optimistic locking across clients
   * @param tables the tables to create/update/delete
   * @return newly created version
   * @throws IOException when the commit fails. The client should update branch locally and retry.
   */
  String commit(String branch,
                CommitMeta commitMeta,
                String version,
                Table... tables) throws IOException;

  /**
   * Delete a branch.
   *
   * <p>
   *   This does not check to see if a branch has been merged fully and it could orphan changes.
   * </p>
   * @param branch name of branch to delete
   * @param version most recent version for optimistic locking
   * @param commitMeta metadata detailing commit changes and owner
   * @throws IOException when version doesn't match the current head
   */
  void deleteBranch(String branch,
                    String version,
                    CommitMeta commitMeta) throws IOException;

  /**
   * Promote commits on mergeBranch into branch.
   *
   * <p>
   *   This allows for merging two branches in a variety of ways:
   *   1. merge (force false, cherryPick false). This takes all commits from mergeBranch and
   *     adds them to branch. This will fail if branch is not a direct ancestor of mergeBranch.
   *   2. force merge (force true, cherryPick false). This will take all commits from mergeBranch
   *     and add them to branch. This will succeed regardless of the state of branch and could
   *     result in data loss.
   *   3. cherryPick (force false, cherryPick true). This will attempt to move changes from
   *     mergeBranch onto branch regardless of the state of branch. This will fail if there are no
   *     conflicts between the branches (defined as tables that have changed in both branches) and
   *     will fail if there are any conflicts. The namespace argument controls whether to do a
   *     full cherry pick or only on the relevant namespace.
   * </p>
   *
   * @param branch branch to merge onto
   * @param mergeBranch branch to merge from
   * @param version version of branch that we are targeting
   * @param commitMeta details of the commit
   * @param force force a merge
   * @param cherryPick do a cherry pick instead
   * @param namespace only perform cherry pick on a specific namespace
   * @return new version of branch
   * @throws IOException when merge can't be completed safely
   */
  String promote(String branch,
                 String mergeBranch,
                 String version,
                 CommitMeta commitMeta,
                 boolean force,
                 boolean cherryPick,
                 String namespace) throws IOException;
}

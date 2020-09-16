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

package com.dremio.nessie.iceberg.branch;

import java.util.Optional;

/**
 * Catalog object for Nessie...akin to iceberg Catalog interface
 */
public interface GitCatalog {

  void createBranch(String branchName, Optional<String> hash);

  boolean deleteBranch(String branchName, String hash);

  void createTag(String tagName, String hash);

  void deleteTag(String tagName, String hash);

  void assignReference(String tagName, String currentHash, String targetHash);

  void refresh();
}

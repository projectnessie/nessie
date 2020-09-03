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
package com.dremio.nessie.server;

import javax.inject.Inject;

import com.dremio.nessie.model.CommitMeta;
import com.dremio.nessie.model.Table;
import com.dremio.nessie.services.TableCommitMetaStoreWorker;
import com.dremio.nessie.versioned.impl.JGitStore;

public class TableCommitMetaJGitStore extends JGitStore<Table, CommitMeta> {

  /**
   * Construct a JGitStore.
   *
   * <p>
   *   This class only exists to make HK2 work w/ generics.
   *   todo remove and switch to CDI
   * </p>
   */
  @Inject
  public TableCommitMetaJGitStore(TableCommitMetaStoreWorker storeWorker) {
    super(storeWorker);
  }
}

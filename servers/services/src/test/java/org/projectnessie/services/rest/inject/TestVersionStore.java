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
package org.projectnessie.services.rest.inject;

import java.util.Optional;

import javax.inject.Singleton;

import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.Contents;
import org.projectnessie.server.store.TableCommitMetaStoreWorker;
import org.projectnessie.versioned.BranchName;
import org.projectnessie.versioned.memory.InMemoryVersionStore;

@Singleton
public class TestVersionStore extends InMemoryVersionStore<Contents, CommitMeta> {

  /**
   * A version-store for tests.
   */
  public TestVersionStore() {
    super(new TableCommitMetaStoreWorker().getValueSerializer(), new TableCommitMetaStoreWorker().getMetadataSerializer());
    try {
      create(BranchName.of("main"), Optional.empty());
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}

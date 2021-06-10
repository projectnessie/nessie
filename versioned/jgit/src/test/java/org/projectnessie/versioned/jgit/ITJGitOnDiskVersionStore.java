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
package org.projectnessie.versioned.jgit;

import java.io.File;
import java.io.IOException;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.projectnessie.versioned.VersionStoreException;
import org.projectnessie.versioned.tests.AbstractITVersionStore;

public class ITJGitOnDiskVersionStore extends AbstractITJGitVersionStore {

  @TempDir File jgitDir;

  @BeforeEach
  void setUp() throws IOException {
    try {
      repository = Git.init().setDirectory(jgitDir).call().getRepository();
    } catch (GitAPIException e) {
      throw new IOException(e);
    }
    store = new JGitVersionStore<>(repository, WORKER);
  }

  @Nested
  @DisplayName("when merging")
  class WhenMerging extends AbstractITVersionStore.WhenMerging {
    @Override
    @Test
    @Disabled
    protected void mergeWithConflictingKeys() throws VersionStoreException {
      super.mergeWithConflictingKeys();
    }
  }
}

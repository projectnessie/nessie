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

import com.dremio.nessie.backend.EntityBackend;
import com.dremio.nessie.model.GitObject;
import com.dremio.nessie.model.GitRef;
import java.io.IOException;
import org.eclipse.jgit.internal.storage.dfs.DfsObjDatabase;
import org.eclipse.jgit.internal.storage.dfs.DfsReaderOptions;
import org.eclipse.jgit.internal.storage.dfs.DfsRepository;
import org.eclipse.jgit.internal.storage.dfs.DfsRepositoryBuilder;
import org.eclipse.jgit.lib.RefDatabase;

public class NessieRepository extends DfsRepository {

  private final NessieObjDatabase objectDb;
  private final NessieRefDatabase refDatabase;
  private final JGitBackend backend;

  /**
   * Builder for in-memory repositories.
   */
  public static class Builder
      extends DfsRepositoryBuilder<NessieRepository.Builder, NessieRepository> {

    private EntityBackend<GitObject> backend;
    private EntityBackend<GitRef> refBackend;

    public Builder setBackend(EntityBackend<GitObject> backend) {
      this.backend = backend;
      return this;
    }

    public Builder setRefBackend(EntityBackend<GitRef> backend) {
      this.refBackend = backend;
      return this;
    }

    @Override
    public NessieRepository build() throws IOException {
      return new NessieRepository(this);
    }
  }

  /**
   * Initialize a DFS repository.
   *
   * @param builder description of the repository.
   */
  protected NessieRepository(DfsRepositoryBuilder builder) {
    super(builder);
    this.backend = new JGitBackend(((Builder) builder).backend, ((Builder) builder).refBackend);
    objectDb = new NessieObjDatabase(this, new DfsReaderOptions(), this.backend);
    refDatabase = new NessieRefDatabase(this, this.backend);
  }


  @Override
  public DfsObjDatabase getObjectDatabase() {
    return objectDb;
  }

  @Override
  public RefDatabase getRefDatabase() {
    return refDatabase;
  }
}

/*
 * Copyright (C) 2017-2019 Dremio Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dremio.iceberg.client.jgit;

import java.io.IOException;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.eclipse.jgit.internal.storage.dfs.DfsRepositoryDescription;
import org.eclipse.jgit.internal.storage.dfs.InMemoryRepository;
import org.eclipse.jgit.transport.RefSpec;

public class JGitContainer {

  private final InMemoryRepository repository;
  private final String baseTag;
  private final Git git;

  public JGitContainer(String baseTag) throws GitAPIException {
    this.baseTag = baseTag;
    DfsRepositoryDescription repoDesc = new DfsRepositoryDescription();
    InMemoryRepository repository;
    try {
      repository = new InMemoryRepository.Builder().setRepositoryDescription(repoDesc).build();
    } catch (IOException e) {
      //pass can't happen
      repository = null;
    }
    this.repository = repository;
    git = new Git(this.repository);
    git.fetch()
       .setRemote("https://github.com/eirslett/test-git-repo.git")
       .setRefSpecs(new RefSpec("+refs/heads/*:refs/heads/*"))
       .call();
    //todo clone from server
//    this.git.
  }

  public static void main(String[] args) throws GitAPIException {
    new JGitContainer(null);
  }
}

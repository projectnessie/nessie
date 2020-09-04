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

import java.time.ZoneId;
import java.time.ZonedDateTime;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

import org.eclipse.jgit.lib.CommitBuilder;
import org.eclipse.jgit.lib.PersonIdent;
import org.eclipse.jgit.lib.Repository;

import com.dremio.nessie.backend.Backend;
import com.dremio.nessie.model.CommitMeta;
import com.dremio.nessie.model.Table;

@ApplicationScoped
public class JgitBranchControllerLegacy extends JgitBranchController<Table, CommitMeta> {

  public JgitBranchControllerLegacy(Repository repository) {
    super(new TableCommitMetaStoreWorker(repository), repository);
  }

  @Inject
  public JgitBranchControllerLegacy(Backend backend) {
    super(TableCommitMetaStoreWorker::new, backend);
  }

  @Override
  protected CommitBuilder fromUser(CommitMeta commitMeta) {
    long updateTime = ZonedDateTime.now(ZoneId.of("UTC")).toInstant().toEpochMilli();
    CommitBuilder commitBuilder = new CommitBuilder();
    PersonIdent person;
    if (commitMeta != null) {
      commitBuilder.setMessage(commitMeta.toMessage());
      person = new PersonIdent(commitMeta.commiter(),
                               commitMeta.email(),
                               updateTime,
                               0);
    } else {
      person = new PersonIdent("test", "me@example.com");
      commitBuilder.setMessage("none");
    }
    commitBuilder.setAuthor(person);
    commitBuilder.setCommitter(person);
    return commitBuilder;
  }
}

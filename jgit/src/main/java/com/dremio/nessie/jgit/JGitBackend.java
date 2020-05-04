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
import com.dremio.nessie.model.Base;
import com.dremio.nessie.model.GitObject;
import com.dremio.nessie.model.GitRef;
import com.dremio.nessie.model.ImmutableGitRef;
import com.dremio.nessie.model.VersionedWrapper;
import java.io.IOException;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.eclipse.jgit.internal.storage.dfs.DfsRefDatabase.RefCache;
import org.eclipse.jgit.lib.AnyObjectId;
import org.eclipse.jgit.lib.ObjectId;
import org.eclipse.jgit.lib.ObjectIdRef.Unpeeled;
import org.eclipse.jgit.lib.Ref;
import org.eclipse.jgit.lib.Ref.Storage;
import org.eclipse.jgit.revwalk.RevCommit;
import org.eclipse.jgit.util.RefList;

public class JGitBackend {

  private final EntityBackend<GitObject> backend;
  private final EntityBackend<GitRef> refBackend;
  private final Map<AnyObjectId, VersionedWrapper<GitObject>> objectCache = new HashMap<>();
  private final Function<AnyObjectId, VersionedWrapper<GitObject>> mappingFunction;
  private final Function<String, VersionedWrapper<GitRef>> mappingFunctionRef;
  private final Map<String, Long> versions = new HashMap<>();
  private RefList<Ref> refs = RefList.emptyList();
  private RefList<Ref> syms = RefList.emptyList();
  private boolean refListBuilt = false;

  public JGitBackend(EntityBackend<GitObject> backend, EntityBackend<GitRef> refBackend) {
    this.backend = backend;
    this.refBackend = refBackend;
    this.mappingFunction = objectId -> this.backend.get(objectId.name());
    this.mappingFunctionRef = this.refBackend::get;
  }

  public boolean contains(AnyObjectId objectId) {
    if (objectCache.containsKey(objectId)) {
      return true;
    } else {
      return objectCache.computeIfAbsent(objectId, mappingFunction) != null;
    }
  }

  public GitObject get(AnyObjectId objectId) {
    VersionedWrapper<GitObject> vw = objectCache.computeIfAbsent(objectId, mappingFunction);
    return vw == null ? null : vw.getObj();
  }

  public VersionedWrapper<GitRef> getRaw(String objectId) {
    return mappingFunctionRef.apply(objectId);
  }

  public void put(GitObject object) {
    VersionedWrapper<GitObject> vw = new VersionedWrapper<>(object);
    try {
      backend.create(object.getId(), vw);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    objectCache.putIfAbsent(ObjectId.fromString(object.getId()), vw);
  }

  public RefCache refList() {
    if (!refListBuilt) {
      List<GitRef> refList = refBackend.getAll(false)
                                       .stream()
                                       .map(VersionedWrapper::getObj)
                                       .collect(Collectors.toList());
      for (GitRef o : refList) {
        refs = refs.put(new Unpeeled(Storage.NETWORK,
                                     o.getId(),
                                     ObjectId.fromString(o.getRefId())));
      }
      this.refListBuilt = true;
    }
    return new RefCache(refs, syms);
  }

  public boolean atomicSwap(Ref oldRef, Ref newRef) {
    if (oldRef.isSymbolic() || newRef.isSymbolic()) {
      throw new UnsupportedOperationException("Unsure yet what to do w/ symbolic refs");
    }
    VersionedWrapper<GitRef> newGitRef;
    long updateTime = ZonedDateTime.now(ZoneId.of("UTC")).toInstant().toEpochMilli();
    RevCommit newCommit = RevCommit.parse(objectCache.computeIfAbsent(newRef.getObjectId(),
                                                                      mappingFunction)
                                                     .getObj()
                                                     .getData());
    if (oldRef.getObjectId() != null && // skip if this is first commmit
        //check to ensure we have only one child and that the child is the old commit
        (newCommit.getParentCount() != 1 || !newCommit.getParent(0)
                                                      .getId()
                                                      .equals(oldRef.getObjectId()))) {
      return false;
    }
    VersionedWrapper<GitRef> oldGitContainer = mappingFunctionRef.apply(oldRef.getName());
    Long oldVersion = versions.get(oldRef.getName());
    if (oldGitContainer != null) {
      GitRef oldGitRef = oldGitContainer.getObj();
      //ensure that the current ref is the same as my parent
      if (!ObjectId.fromString(oldGitRef.getRefId()).equals(oldRef.getObjectId())) {
        return false;
      }
    }
    newGitRef = new VersionedWrapper<>(ImmutableGitRef.builder()
                                                      .updateTime(updateTime)
                                                      .id(newRef.getName())
                                                      .refId(newRef.getObjectId().name())
                                                      .isDeleted(false)
                                                      .build(), oldVersion);
    try {
      refBackend.create(newGitRef.getObj().getId(), newGitRef);
    } catch (IOException e) {
      throw new IllegalStateException(
        "Unable to complete commit and update Ref " + newGitRef.getObj().getId(), e);
    }
    return true;
  }

  public boolean atomicRemove(Ref oldRef) {
    refBackend.remove(oldRef.getName());
    return true; //todo I am not sure of when this is called and how 'atomic' this should be.
    // This does not check if the ref exists or if it is at the correct version
  }

  public void putAll(Set<GitObject> transactionSet) {
    backend.updateAll(transactionSet.stream().collect(
        Collectors.toMap(
          Base::getId,
          VersionedWrapper::new
        )));
  }

  public void setVersion(Ref ref, Long version) {
    if (version != null) {
      versions.put(ref.getName(), version);
    }
  }
}

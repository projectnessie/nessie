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

package com.dremio.iceberg.server.jgit;

import com.dremio.iceberg.backend.EntityBackend;
import com.dremio.iceberg.model.GitContainer;
import com.dremio.iceberg.model.GitRef;
import com.dremio.iceberg.model.ImmutableGitRef;
import com.dremio.iceberg.model.VersionedWrapper;
import java.io.IOException;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.eclipse.jgit.internal.storage.dfs.DfsRefDatabase.RefCache;
import org.eclipse.jgit.lib.AnyObjectId;
import org.eclipse.jgit.lib.ObjectId;
import org.eclipse.jgit.lib.ObjectIdRef.Unpeeled;
import org.eclipse.jgit.lib.Ref;
import org.eclipse.jgit.lib.Ref.Storage;
import org.eclipse.jgit.lib.SymbolicRef;
import org.eclipse.jgit.revwalk.DepthWalk.Commit;
import org.eclipse.jgit.revwalk.RevCommit;
import org.eclipse.jgit.util.RefList;

public class JGitBackend {

  private final EntityBackend<GitContainer> backend;
  private final Map<AnyObjectId, VersionedWrapper<GitContainer>> objectCache = new HashMap<>();
  private final Function<AnyObjectId, VersionedWrapper<GitContainer>> mappingFunction;
  private final Function<String, VersionedWrapper<GitContainer>> mappingFunctionRef;
  private RefList<Ref> refs = RefList.emptyList();
  private RefList<Ref> syms = RefList.emptyList();
  private boolean refListBuilt = false;

  public JGitBackend(EntityBackend<GitContainer> backend) {
    this.backend = backend;
    this.mappingFunction = objectId -> this.backend.get(objectId.name(), "obj");
    this.mappingFunctionRef = objectName -> this.backend.get(objectName, "ref");
  }

  public boolean contains(AnyObjectId objectId) {
    if (objectCache.containsKey(objectId)) {
      return true;
    } else {
      return objectCache.computeIfAbsent(objectId, mappingFunction) != null;
    }
  }

  public GitContainer get(AnyObjectId objectId) {
    VersionedWrapper<GitContainer> vw = objectCache.computeIfAbsent(objectId, mappingFunction);
    return vw == null ? null : vw.getObj();
  }

  public void put(GitContainer object) throws IOException {
    VersionedWrapper<GitContainer> vw = new VersionedWrapper<>(object);
    backend.create(object.getId(), vw);
    objectCache.putIfAbsent(object.getObjectId(), vw);
  }

  public RefCache refList() {
    if (!refListBuilt) {
      List<GitContainer> refList = backend.getAll("ref", false)
                                          .stream()
                                          .map(VersionedWrapper::getObj)
                                          .collect(Collectors.toList());
      for (GitContainer o : refList) {
        refs = refs.put(new Unpeeled(Storage.NETWORK, o.getId(), (ObjectId) o.getObjectId()));
        if (o.getTargetRef() != null) {
          syms = syms.put(new SymbolicRef(o.getId(), o.getTargetRef()));
          refs = refs.put(new Unpeeled(Storage.NETWORK,
                                       o.getTargetRef().getName(),
                                       o.getTargetRef().getObjectId()));
        }
      }
      this.refListBuilt = true;
    }
    return new RefCache(refs, syms);
  }

  public boolean atomicSwap(Ref oldRef, Ref newRef) {
    if (oldRef.isSymbolic() || newRef.isSymbolic()) {
      throw new UnsupportedOperationException("Unsure yet what to do w/ symbolic refs");
    }
    VersionedWrapper<GitContainer> newGitRef = null;
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
    VersionedWrapper<GitContainer> oldGitContainer = mappingFunctionRef.apply(oldRef.getName());
    if (oldGitContainer != null) {
      GitRef oldGitRef = (GitRef) oldGitContainer.getObj();
      //ensure that the current ref is the same as my parent
      if (!oldGitRef.getRef().getObjectId().equals(oldRef.getObjectId())) {
        return false;
      }
      newGitRef = oldGitContainer.update(ImmutableGitRef.builder()
                                                        .from(oldGitRef)
                                                        .ref(newRef)
                                                        .updateTime(updateTime)
                                                        .build());
    }
    if (newGitRef == null) {
      newGitRef = new VersionedWrapper<>(ImmutableGitRef.builder()
                                                        .updateTime(updateTime)
                                                        .ref(newRef)
                                                        .targetRef(null)
                                                        .isDeleted(false)
                                                        .build());
    }
    backend.create(newGitRef.getObj().getId(), newGitRef);
    return true;
  }

  public boolean atomicRemove(Ref oldRef) {
    String target =
      oldRef.isSymbolic() ? oldRef.getTarget().getName() : oldRef.getObjectId().name();
    backend.remove(target);
    return true; //todo I am not sure of when this is called and how 'atomic' this should be.
    // This does not check if the ref exists or if it is at the correct version
  }
}

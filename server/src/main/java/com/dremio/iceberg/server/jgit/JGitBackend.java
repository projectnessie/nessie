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
import com.dremio.iceberg.model.ImmutableGitObject;
import com.dremio.iceberg.model.ImmutableGitRef;
import com.dremio.iceberg.model.VersionedWrapper;
import java.io.IOException;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.ws.rs.NotAcceptableException;
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
  private final Map<String, VersionedWrapper<GitContainer>> refCache = new HashMap<>();
  private final Function<AnyObjectId, VersionedWrapper<GitContainer>> mappingFunction;
  private final Function<String, VersionedWrapper<GitContainer>> mappingFunctionRef;
  private RefList<Ref> refs = RefList.emptyList();
  private RefList<Ref> syms = RefList.emptyList();
  private boolean refListBuilt = false;

  public JGitBackend(EntityBackend<GitContainer> backend) {
    this.backend = backend;
    this.mappingFunction = objectId -> this.backend.get(objectId.name());
    this.mappingFunctionRef = this.backend::get;
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
//    if (object.getType() == Constants.OBJ_COMMIT) {
//      putCommit(object);
//    } else {
      VersionedWrapper<GitContainer> vw = new VersionedWrapper<>(object);
      backend.create(object.getId(), vw);
      objectCache.putIfAbsent(object.getObjectId(), vw);
//    }
  }

  private void putCommit(GitContainer object) throws IOException {
    RevCommit commit = Commit.parse(object.getData());
    if (commit.getParentCount() == 0) {
      VersionedWrapper<GitContainer> vw = new VersionedWrapper<>(object);
      backend.create(object.getId(), vw);
      objectCache.putIfAbsent(object.getObjectId(), vw);
    } else {
      System.out.println("foo");
    }
    //todo
  }

  public RefCache refList() {
    if (!refListBuilt) {
      List<GitContainer> refList = backend.getAll("ref", false)
                                 .stream()
                                 .map(VersionedWrapper::getObj)
                                       .collect(Collectors.toList());
      for (GitContainer o: refList) {
        refs = refs.put(new Unpeeled(Storage.NETWORK, o.getId(), (ObjectId)o.getObjectId()));
        if (o.getTargetRef() != null) {
          syms = syms.put(new SymbolicRef(o.getId(), o.getTargetRef()));
          refs = refs.put(new Unpeeled(Storage.NETWORK, o.getTargetRef().getName(), o.getTargetRef().getObjectId()));
        }
      }
      this.refListBuilt = true;
    }
    return new RefCache(refs, syms);
  }

  public boolean atomicSwap(Ref oldRef, Ref newRef) {
    String target = newRef.isSymbolic() ? newRef.getTarget().getName() :
      newRef.getObjectId().name();
    String oldTarget = oldRef.isSymbolic() ? oldRef.getTarget().getName() :
      Optional.ofNullable(oldRef.getObjectId()).map(AnyObjectId::name).orElse(null);
    long updateTime = ZonedDateTime.now(ZoneId.of("UTC")).toInstant().toEpochMilli();
    //todo make atomic (sort out versionedwrapper)
    if (oldTarget == null) {
      VersionedWrapper<GitContainer> vw = new VersionedWrapper<>(
        ImmutableGitRef.builder()
                       .updateTime(updateTime)
                       .ref(newRef)
                       .targetRef(newRef.isSymbolic() ? newRef.getTarget() : null)
                       .isDeleted(false)
                       .build()
      );
      backend.create(vw.getObj().getId(), vw);
      refCache.put(vw.getObj().getId(), mappingFunctionRef.apply(vw.getObj().getId()));
    } else {
      RevCommit newCommit = RevCommit.parse(objectCache.computeIfAbsent(newRef.getObjectId(), mappingFunction).getObj().getData());
      if (newCommit.getParentCount() != 1 || !newCommit.getParent(0).getId().equals(oldRef.getObjectId())) {
        return false;
        //throw new NotAcceptableException("Old commit " + oldRef.toString() + " is not the parent of this commit " + newRef.toString());
      }
      VersionedWrapper<GitContainer> vw = new VersionedWrapper<>(
        ImmutableGitRef.builder()
                       .updateTime(updateTime)
                       .ref(newRef)
                       .targetRef(newRef.isSymbolic() ? newRef.getTarget() : null)
                       .isDeleted(false)
                       .build()
      );
      backend.create(vw.getObj().getId(), vw);
      refCache.put(vw.getObj().getId(), mappingFunctionRef.apply(vw.getObj().getId()));
    }
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

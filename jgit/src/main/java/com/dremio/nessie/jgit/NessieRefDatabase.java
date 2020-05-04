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
import com.dremio.nessie.model.BranchControllerReference;
import com.dremio.nessie.model.ImmutableBranchControllerReference;
import com.dremio.nessie.model.VersionedWrapper;
import java.io.IOException;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.eclipse.jgit.internal.storage.dfs.DfsRefDatabase;
import org.eclipse.jgit.internal.storage.dfs.DfsRepository;
import org.eclipse.jgit.lib.ObjectId;
import org.eclipse.jgit.lib.ObjectIdRef.Unpeeled;
import org.eclipse.jgit.lib.Ref;
import org.eclipse.jgit.lib.Ref.Storage;
import org.eclipse.jgit.util.RefList;

public class NessieRefDatabase extends DfsRefDatabase {

  private final EntityBackend<BranchControllerReference> backend;
  private final Map<String,
      VersionedWrapper<BranchControllerReference>> refCache = new HashMap<>();
  private final Function<String, VersionedWrapper<BranchControllerReference>> mappingFunction;

  private RefList<Ref> refs = RefList.emptyList();
  private RefList<Ref> syms = RefList.emptyList();
  private boolean refListBuilt = false;
  /**
   * DfsRefDatabase used by InMemoryRepository.
   */
  boolean performsAtomicTransactions = true;

  /**
   * Initialize a new in-memory ref database.
   */
  protected NessieRefDatabase(DfsRepository repository,
                              EntityBackend<BranchControllerReference> backend) {
    super(repository);
    this.backend = backend;
    this.mappingFunction = this.backend::get;
  }

  @Override
  protected RefCache scanAllRefs() {
    return refList();
  }

  private RefCache refList() {
    if (!refListBuilt) {
      List<BranchControllerReference> refList = backend.getAll(false)
                                                       .stream()
                                                       .map(VersionedWrapper::getObj)
                                                       .collect(Collectors.toList());
      for (BranchControllerReference o : refList) {
        refs = refs.put(new Unpeeled(Storage.NETWORK,
                                     o.getId(),
                                     ObjectId.fromString(o.getRefId())));
      }
      this.refListBuilt = true;
    }
    return new RefCache(refs, syms);
  }

  @Override
  protected boolean compareAndPut(Ref oldRef, Ref newRef) {
    return atomicSwap(oldRef, newRef);
  }


  @Override
  protected boolean compareAndRemove(Ref oldRef) {
    return atomicRemove(oldRef);
  }

  @Override
  public boolean performsAtomicTransactions() {
    return performsAtomicTransactions;
  }

  private boolean atomicSwap(Ref oldRef, Ref newRef) {
    if (oldRef.isSymbolic() || newRef.isSymbolic()) {
      throw new UnsupportedOperationException("Unsure yet what to do w/ symbolic refs");
    }
    VersionedWrapper<BranchControllerReference> newGitRef;
    long updateTime = ZonedDateTime.now(ZoneId.of("UTC")).toInstant().toEpochMilli();
    VersionedWrapper<BranchControllerReference> oldGitContainer =
        mappingFunction.apply(oldRef.getName());
    Long oldVersion = Optional.ofNullable(refCache.get(oldRef.getName()))
                              .map(VersionedWrapper::getVersion)
                              .orElse(null);
    if (oldGitContainer != null) {
      BranchControllerReference oldGitRef = oldGitContainer.getObj();
      //ensure that the current ref is the same as my parent
      if (!ObjectId.fromString(oldGitRef.getRefId()).equals(oldRef.getObjectId())) {
        return false;
      }
    }
    newGitRef = new VersionedWrapper<>(
      ImmutableBranchControllerReference.builder()
                                        .updateTime(updateTime)
                                        .id(newRef.getName())
                                        .refId(newRef.getObjectId().name())
                                        .build(), oldVersion);
    try {
      backend.create(newGitRef.getObj().getId(), newGitRef);
    } catch (IOException e) {
      throw new IllegalStateException(
        "Unable to complete commit and update Ref " + newGitRef.getObj().getId(), e);
    }
    return true;
  }

  private boolean atomicRemove(Ref oldRef) {
    backend.remove(oldRef.getName());
    return true; //todo I am not sure of when this is called and how 'atomic' this should be.
    // This does not check if the ref exists or if it is at the correct version
  }
}

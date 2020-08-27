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
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import org.eclipse.jgit.errors.MissingObjectException;
import org.eclipse.jgit.internal.storage.dfs.DfsObjDatabase;
import org.eclipse.jgit.internal.storage.dfs.DfsOutputStream;
import org.eclipse.jgit.internal.storage.dfs.DfsPackDescription;
import org.eclipse.jgit.internal.storage.dfs.DfsReader;
import org.eclipse.jgit.internal.storage.dfs.DfsReaderOptions;
import org.eclipse.jgit.internal.storage.dfs.DfsRepository;
import org.eclipse.jgit.internal.storage.dfs.ReadableChannel;
import org.eclipse.jgit.internal.storage.pack.PackExt;
import org.eclipse.jgit.lib.AnyObjectId;
import org.eclipse.jgit.lib.Constants;
import org.eclipse.jgit.lib.ObjectId;
import org.eclipse.jgit.lib.ObjectInserter;
import org.eclipse.jgit.lib.ObjectLoader.SmallObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.nessie.backend.EntityBackend;
import com.dremio.nessie.model.BranchControllerObject;
import com.dremio.nessie.model.ImmutableBranchControllerObject;
import com.dremio.nessie.model.VersionedWrapper;

/**
 * Object databse for Nessie. This uses dynamodb to store git objects.
 */
public class NessieObjDatabase extends DfsObjDatabase {
  private static final Logger logger = LoggerFactory.getLogger(NessieObjDatabase.class);
  private final EntityBackend<BranchControllerObject> backend;
  private final Map<AnyObjectId,
      VersionedWrapper<BranchControllerObject>> objectCache = new HashMap<>();
  private final Function<AnyObjectId, VersionedWrapper<BranchControllerObject>> mappingFunction;

  private final Set<BranchControllerObject> transactionSet = new HashSet<>();

  /**
   * Initialize an object database for our repository.
   *
   * @param repository repository owning this object database.
   */
  protected NessieObjDatabase(DfsRepository repository,
                              DfsReaderOptions options,
                              EntityBackend<BranchControllerObject> backend) {
    super(repository, options);
    this.mappingFunction = objectId -> backend.get(objectId.name());
    this.backend = backend;
  }

  @Override
  public DfsReader newReader() {
    return new NessieObjReader(this);
  }

  @Override
  public ObjectInserter newInserter() {
    return new NessieObjectInserter(this);
  }

  @Override
  protected DfsPackDescription newPack(PackSource source) {
    throw new UnsupportedOperationException("Not yet implemented");
  }

  @Override
  protected void commitPackImpl(Collection<DfsPackDescription> desc,
                                Collection<DfsPackDescription> replaces) {
    throw new UnsupportedOperationException("Not yet implemented");
  }

  @Override
  protected void rollbackPack(Collection<DfsPackDescription> desc) {
    throw new UnsupportedOperationException("Not yet implemented");
  }

  @Override
  protected List<DfsPackDescription> listPacks() {
    throw new UnsupportedOperationException("Not yet implemented");
  }

  @Override
  protected ReadableChannel openFile(DfsPackDescription desc,
                                     PackExt ext) {
    throw new UnsupportedOperationException("Not yet implemented");
  }

  @Override
  protected DfsOutputStream writeFile(DfsPackDescription desc,
                                      PackExt ext) {
    throw new UnsupportedOperationException("Not yet implemented");
  }

  @Override
  public boolean has(AnyObjectId objectId) {
    return contains(objectId);
  }

  @Override
  public boolean has(AnyObjectId objectId, boolean avoidUnreachableObjects) {
    return contains(objectId);
  }

  private boolean contains(AnyObjectId objectId) {
    if (objectCache.containsKey(objectId)) {
      return true;
    } else {
      return objectCache.computeIfAbsent(objectId, mappingFunction) != null;
    }
  }

  void put(AnyObjectId objectId, int type, byte[] data) {
    long updateTime = ZonedDateTime.now(ZoneId.of("UTC")).toInstant().toEpochMilli();
    BranchControllerObject object = ImmutableBranchControllerObject.builder()
                                                                   .id(objectId.name())
                                                                   .data(data)
                                                                   .type(type)
                                                                   .updateTime(updateTime)
                                                                   .build();
    put(object);
  }

  private void put(BranchControllerObject object) {
    VersionedWrapper<BranchControllerObject> vw = new VersionedWrapper<>(object);
    backend.update(object.getId(), vw);
    objectCache.putIfAbsent(ObjectId.fromString(object.getId()), vw);
  }

  void putAll(AnyObjectId objectId, int type, byte[] data) {
    long updateTime = ZonedDateTime.now(ZoneId.of("UTC")).toInstant().toEpochMilli();
    BranchControllerObject object = ImmutableBranchControllerObject.builder()
                                                                   .id(objectId.name())
                                                                   .data(data)
                                                                   .type(type)
                                                                   .updateTime(updateTime)
                                                                   .build();
    transactionSet.add(object);
  }

  private void putAll(Set<BranchControllerObject> transactionSet) {
    Map<String, VersionedWrapper<BranchControllerObject>> updateMap = new HashMap<>();
    Set<BranchControllerObject> updateMapDups = new HashSet<>();
    for (BranchControllerObject b: transactionSet) {
      if (updateMap.containsKey(b.getId())) {
        logger.error("Duplicate Key in update: {} with types {} and {}",
                     b.getId(),
                     b.getType(),
                     updateMap.get(b.getId()).getObj().getType());
        updateMapDups.add(b);
      } else {
        updateMap.put(b.getId(), new VersionedWrapper<>(b));
      }
    }
    backend.updateAll(updateMap);
    if (!updateMapDups.isEmpty()) {
      putAll(updateMapDups);
    }
  }

  void flush() {
    if (transactionSet.isEmpty()) {
      return;
    }
    try {
      putAll(transactionSet);
    } finally {
      transactionSet.clear();
    }
  }

  /**
   * Get data from the object store.
   *
   * @param objectId id of object to retrieve
   * @param typeHint expected type
   * @return object containing stored data
   * @throws MissingObjectException when the object does not exist in dynamodb
   */
  public SmallObject get(AnyObjectId objectId, int typeHint) throws MissingObjectException {
    if (objectId.equals(ObjectId.zeroId())) {
      return new SmallObject(typeHint, new byte[0]);
    }
    BranchControllerObject obj = get(objectId);
    if (obj == null) {
      String typeHintStr;
      try {
        typeHintStr = Constants.typeString(typeHint);
      } catch (IllegalArgumentException e) {
        typeHintStr = "ANY";
      }
      throw new MissingObjectException((ObjectId) objectId, typeHintStr);
    }
    return new SmallObject(obj.getType(), obj.getData());
  }

  /**
   * Retrieves an object from the object store and wraps it in a model object.
   *
   * <p>
   *   This uses a simple cache to prevent duplicate fetches of same object. The objects
   *   are immutable so this does not need to be managed.
   * </p>
   * @param objectId id of the object to retrieve
   * @return model object of the git object
   */
  public BranchControllerObject get(AnyObjectId objectId) {
    VersionedWrapper<BranchControllerObject> vw =
        objectCache.computeIfAbsent(objectId, mappingFunction);
    return vw == null ? null : vw.getObj();
  }

}

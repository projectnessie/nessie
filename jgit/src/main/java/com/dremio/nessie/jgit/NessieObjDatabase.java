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

import com.dremio.nessie.model.GitObject;
import com.dremio.nessie.model.ImmutableGitObject;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.OptionalLong;
import java.util.Set;
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
import org.eclipse.jgit.lib.ObjectId;
import org.eclipse.jgit.lib.ObjectInserter;
import org.eclipse.jgit.lib.ObjectLoader.SmallObject;

public class NessieObjDatabase extends DfsObjDatabase {

  private final JGitBackend backend;
  private final Set<GitObject> transactionSet = new HashSet<>();

  /**
   * Initialize an object database for our repository.
   *
   * @param repository repository owning this object database.
   */
  protected NessieObjDatabase(DfsRepository repository,
                              DfsReaderOptions options,
                              JGitBackend backend) {
    super(repository, options);
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
  protected DfsPackDescription newPack(PackSource source) throws IOException {
    return null;
  }

  @Override
  protected void commitPackImpl(Collection<DfsPackDescription> desc,
                                Collection<DfsPackDescription> replaces) throws IOException {

  }

  @Override
  protected void rollbackPack(Collection<DfsPackDescription> desc) {

  }

  @Override
  protected List<DfsPackDescription> listPacks() throws IOException {
    return null;
  }

  @Override
  protected ReadableChannel openFile(DfsPackDescription desc,
                                     PackExt ext) throws FileNotFoundException, IOException {
    return null;
  }

  @Override
  protected DfsOutputStream writeFile(DfsPackDescription desc,
                                      PackExt ext) throws IOException {
    return null;
  }

  @Override
  public boolean has(AnyObjectId objectId) throws IOException {
    return super.has(objectId);
  }

  @Override
  public boolean has(AnyObjectId objectId, boolean avoidUnreachableObjects) throws IOException {
    return backend.contains(objectId);
  }

  void put(AnyObjectId objectId, int type, byte[] data) throws IOException {
    long updateTime = ZonedDateTime.now(ZoneId.of("UTC")).toInstant().toEpochMilli();
    GitObject object = ImmutableGitObject.builder()
                                            .id(objectId.name())
                                            .data(data)
                                            .isDeleted(false)
                                            .type(type)
                                            .updateTime(updateTime)
                                            .build();
    backend.put(object);
  }

  void putAll(AnyObjectId objectId, int type, byte[] data) throws IOException {
    long updateTime = ZonedDateTime.now(ZoneId.of("UTC")).toInstant().toEpochMilli();
    GitObject object = ImmutableGitObject.builder()
                                         .id(objectId.name())
                                         .data(data)
                                         .isDeleted(false)
                                         .type(type)
                                         .updateTime(updateTime)
                                         .build();
    transactionSet.add(object);
  }

  void flush() {
    if (transactionSet.isEmpty()) {
      return;
    }
    try {
      backend.putAll(transactionSet);
    } finally {
      transactionSet.clear();
    }
  }

  public SmallObject get(AnyObjectId objectId, int typeHint) throws MissingObjectException {
    GitObject obj = backend.get(objectId);
    if (obj == null) {
      throw new MissingObjectException((ObjectId) objectId, typeHint);
    }
    return new SmallObject(obj.getType(), obj.getData());
  }

  public OptionalLong getVersion(String objectId) {
    return backend.getRaw(objectId).getVersion();
  }
}

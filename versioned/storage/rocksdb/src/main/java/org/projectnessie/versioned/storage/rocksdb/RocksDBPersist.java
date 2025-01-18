/*
 * Copyright (C) 2022 Dremio
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
package org.projectnessie.versioned.storage.rocksdb;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Collections.singleton;
import static org.projectnessie.versioned.storage.rocksdb.RocksDBBackend.keyPrefix;
import static org.projectnessie.versioned.storage.rocksdb.RocksDBBackend.rocksDbException;
import static org.projectnessie.versioned.storage.serialize.ProtoSerialization.deserializeObj;
import static org.projectnessie.versioned.storage.serialize.ProtoSerialization.deserializeObjId;
import static org.projectnessie.versioned.storage.serialize.ProtoSerialization.deserializeReference;
import static org.projectnessie.versioned.storage.serialize.ProtoSerialization.serializeObj;
import static org.projectnessie.versioned.storage.serialize.ProtoSerialization.serializeReference;

import com.google.common.collect.AbstractIterator;
import jakarta.annotation.Nonnull;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.function.Predicate;
import org.projectnessie.nessie.relocated.protobuf.ByteString;
import org.projectnessie.versioned.storage.common.config.StoreConfig;
import org.projectnessie.versioned.storage.common.exceptions.ObjNotFoundException;
import org.projectnessie.versioned.storage.common.exceptions.ObjTooLargeException;
import org.projectnessie.versioned.storage.common.exceptions.RefAlreadyExistsException;
import org.projectnessie.versioned.storage.common.exceptions.RefConditionFailedException;
import org.projectnessie.versioned.storage.common.exceptions.RefNotFoundException;
import org.projectnessie.versioned.storage.common.objtypes.UpdateableObj;
import org.projectnessie.versioned.storage.common.persist.CloseableIterator;
import org.projectnessie.versioned.storage.common.persist.Obj;
import org.projectnessie.versioned.storage.common.persist.ObjId;
import org.projectnessie.versioned.storage.common.persist.ObjType;
import org.projectnessie.versioned.storage.common.persist.Persist;
import org.projectnessie.versioned.storage.common.persist.Reference;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.TransactionDB;

class RocksDBPersist implements Persist {

  private final RocksDBBackend backend;
  private final RocksDBRepo repo;
  private final StoreConfig config;

  private final ByteString keyPrefix;

  RocksDBPersist(RocksDBBackend backend, RocksDBRepo repo, StoreConfig config) {
    this.backend = backend;
    this.repo = repo;
    this.config = config;
    this.keyPrefix = keyPrefix(config.repositoryId());
  }

  private byte[] dbKey(ByteString key) {
    return keyPrefix.concat(key).toByteArray();
  }

  private byte[] dbKey(String key) {
    return dbKey(ByteString.copyFromUtf8(key));
  }

  private byte[] dbKey(ObjId id) {
    return dbKey(id.asBytes());
  }

  @Nonnull
  @Override
  public String name() {
    return RocksDBBackendFactory.NAME;
  }

  @Override
  @Nonnull
  public StoreConfig config() {
    return config;
  }

  @Override
  public Reference fetchReference(@Nonnull String name) {
    try {
      RocksDBBackend v = backend;
      TransactionDB db = v.db();
      ColumnFamilyHandle cf = v.refs();
      byte[] key = dbKey(name);

      byte[] reference = db.get(cf, key);
      return deserializeReference(reference);
    } catch (RocksDBException e) {
      throw rocksDbException(e);
    }
  }

  @Override
  @Nonnull
  public Reference[] fetchReferences(@Nonnull String[] names) {
    try {
      RocksDBBackend v = backend;
      TransactionDB db = v.db();
      ColumnFamilyHandle cf = v.refs();

      int num = names.length;
      Reference[] r = new Reference[num];
      List<ColumnFamilyHandle> handles = new ArrayList<>(num);
      List<byte[]> keys = new ArrayList<>(num);
      for (String name : names) {
        if (name != null) {
          handles.add(cf);
          keys.add(dbKey(name));
        }
      }

      if (!keys.isEmpty()) {
        List<byte[]> dbResult = db.multiGetAsList(handles, keys);

        for (int i = 0, ri = 0; i < num; i++) {
          String name = names[i];
          if (name != null) {
            byte[] reference = dbResult.get(ri++);
            r[i] = deserializeReference(reference);
          }
        }
      }

      return r;
    } catch (RocksDBException e) {
      throw rocksDbException(e);
    }
  }

  @Override
  @Nonnull
  public Reference addReference(@Nonnull Reference reference) throws RefAlreadyExistsException {
    checkArgument(!reference.deleted(), "Deleted references must not be added");

    Lock l = repo.referencesLock(reference.name());
    try {
      RocksDBBackend b = backend;
      TransactionDB db = b.db();
      ColumnFamilyHandle cf = b.refs();
      byte[] key = dbKey(reference.name());

      byte[] existing = db.get(cf, key);
      if (existing != null) {
        throw new RefAlreadyExistsException(deserializeReference(existing));
      }

      db.put(cf, key, serializeReference(reference));

      return reference;
    } catch (RocksDBException e) {
      throw rocksDbException(e);
    } finally {
      l.unlock();
    }
  }

  @Override
  @Nonnull
  public Reference markReferenceAsDeleted(@Nonnull Reference reference)
      throws RefNotFoundException, RefConditionFailedException {
    Lock l = repo.referencesLock(reference.name());
    try {
      RocksDBBackend b = backend;
      TransactionDB db = b.db();
      ColumnFamilyHandle cf = b.refs();
      byte[] key = dbKey(reference.name());

      checkReference(reference, db, cf, key, false);

      Reference asDeleted = reference.withDeleted(true);
      db.put(cf, key, serializeReference(asDeleted));
      return asDeleted;
    } catch (RocksDBException e) {
      throw rocksDbException(e);
    } finally {
      l.unlock();
    }
  }

  private static void checkReference(
      Reference expected,
      TransactionDB db,
      ColumnFamilyHandle cf,
      byte[] key,
      boolean expectDeleted)
      throws RocksDBException, RefNotFoundException, RefConditionFailedException {
    byte[] existing = db.get(cf, key);
    if (existing == null) {
      throw new RefNotFoundException(expected);
    }

    Reference ref = deserializeReference(existing);
    if (ref.deleted() != expectDeleted || !ref.equals(expected)) {
      throw new RefConditionFailedException(ref);
    }
  }

  @Override
  public void purgeReference(@Nonnull Reference reference)
      throws RefNotFoundException, RefConditionFailedException {
    Lock l = repo.referencesLock(reference.name());
    try {
      RocksDBBackend b = backend;
      TransactionDB db = b.db();
      ColumnFamilyHandle cf = b.refs();
      byte[] key = dbKey(reference.name());

      checkReference(reference.withDeleted(true), db, cf, key, true);

      db.delete(cf, key);
    } catch (RocksDBException e) {
      throw rocksDbException(e);
    } finally {
      l.unlock();
    }
  }

  @Override
  @Nonnull
  public Reference updateReferencePointer(@Nonnull Reference reference, @Nonnull ObjId newPointer)
      throws RefNotFoundException, RefConditionFailedException {
    Lock l = repo.referencesLock(reference.name());
    try {
      RocksDBBackend b = backend;
      TransactionDB db = b.db();
      ColumnFamilyHandle cf = b.refs();
      byte[] key = dbKey(reference.name());

      checkReference(reference, db, cf, key, false);

      Reference updated = reference.forNewPointer(newPointer, config);

      db.put(cf, key, serializeReference(updated));
      return updated;
    } catch (RocksDBException e) {
      throw rocksDbException(e);
    } finally {
      l.unlock();
    }
  }

  @Override
  @Nonnull
  public <T extends Obj> T fetchTypedObj(
      @Nonnull ObjId id, ObjType type, @Nonnull Class<T> typeClass) throws ObjNotFoundException {

    try {
      RocksDBBackend b = backend;
      TransactionDB db = b.db();
      ColumnFamilyHandle cf = b.objs();
      byte[] key = dbKey(id);

      byte[] obj = db.get(cf, key);
      if (obj == null) {
        throw new ObjNotFoundException(id);
      }
      Obj o = deserializeObj(id, 0L, obj, null);
      if (o == null || (type != null && !type.equals(o.type()))) {
        throw new ObjNotFoundException(id);
      }
      @SuppressWarnings("unchecked")
      T typed = (T) o;
      return typed;
    } catch (RocksDBException e) {
      throw rocksDbException(e);
    }
  }

  @Override
  public <T extends Obj> T[] fetchTypedObjsIfExist(
      @Nonnull ObjId[] ids, ObjType type, @Nonnull Class<T> typeClass) {
    try {
      RocksDBBackend b = backend;
      TransactionDB db = b.db();
      ColumnFamilyHandle cf = b.objs();

      int num = ids.length;
      @SuppressWarnings("unchecked")
      T[] r = (T[]) Array.newInstance(typeClass, num);
      List<ColumnFamilyHandle> handles = new ArrayList<>(num);
      List<byte[]> keys = new ArrayList<>(num);
      for (ObjId id : ids) {
        if (id != null) {
          handles.add(cf);
          keys.add(dbKey(id));
        }
      }

      if (!keys.isEmpty()) {
        List<byte[]> dbResult = db.multiGetAsList(handles, keys);
        for (int i = 0, ri = 0; i < num; i++) {
          ObjId id = ids[i];
          if (id != null) {
            byte[] obj = dbResult.get(ri++);
            if (obj != null) {
              Obj o = deserializeObj(id, 0L, obj, null);
              if (type != null && !type.equals(o.type())) {
                o = null;
              }
              @SuppressWarnings("unchecked")
              T typed = (T) o;
              r[i] = typed;
            }
          }
        }
      }

      return r;
    } catch (RocksDBException e) {
      throw rocksDbException(e);
    }
  }

  @Override
  public boolean storeObj(@Nonnull Obj obj, boolean ignoreSoftSizeRestrictions)
      throws ObjTooLargeException {
    checkArgument(obj.id() != null, "Obj to store must have a non-null ID");

    Lock l = repo.objLock(obj.id());
    try {
      RocksDBBackend b = backend;
      TransactionDB db = b.db();
      ColumnFamilyHandle cf = b.objs();
      byte[] key = dbKey(obj.id());

      long referenced = config.currentTimeMicros();
      boolean r;

      byte[] existing = db.get(cf, key);
      if (existing != null) {
        obj = deserializeObj(obj.id(), referenced, existing, null);
        ignoreSoftSizeRestrictions = true;
        r = false;
      } else {
        var objReferenced = obj.referenced();
        // -1 is a sentinel for AbstractBasePersistTests.deleteWithReferenced()
        obj = obj.withReferenced(objReferenced != -1L ? referenced : -1L);
        r = true;
      }

      int incrementalIndexSizeLimit =
          ignoreSoftSizeRestrictions ? Integer.MAX_VALUE : effectiveIncrementalIndexSizeLimit();
      int indexSizeLimit =
          ignoreSoftSizeRestrictions ? Integer.MAX_VALUE : effectiveIndexSegmentSizeLimit();
      byte[] serialized = serializeObj(obj, incrementalIndexSizeLimit, indexSizeLimit, true);

      db.put(cf, key, serialized);
      return r;
    } catch (RocksDBException e) {
      throw rocksDbException(e);
    } finally {
      l.unlock();
    }
  }

  @Override
  @Nonnull
  public boolean[] storeObjs(@Nonnull Obj[] objs) throws ObjTooLargeException {
    boolean[] r = new boolean[objs.length];
    for (int i = 0; i < objs.length; i++) {
      Obj o = objs[i];
      if (o != null) {
        r[i] = storeObj(o, false);
      }
    }
    return r;
  }

  @Override
  public void deleteObj(@Nonnull ObjId id) {
    Lock l = repo.objLock(id);
    try {
      RocksDBBackend b = backend;
      TransactionDB db = b.db();
      ColumnFamilyHandle cf = b.objs();
      byte[] key = dbKey(id);

      db.delete(cf, key);
    } catch (RocksDBException e) {
      throw rocksDbException(e);
    } finally {
      l.unlock();
    }
  }

  @Override
  public void deleteObjs(@Nonnull ObjId[] ids) {
    for (ObjId id : ids) {
      if (id != null) {
        deleteObj(id);
      }
    }
  }

  @Override
  public void upsertObj(@Nonnull Obj obj) throws ObjTooLargeException {
    ObjId id = obj.id();
    checkArgument(id != null, "Obj to store must have a non-null ID");

    Lock l = repo.objLock(obj.id());
    try {
      RocksDBBackend b = backend;
      TransactionDB db = b.db();
      ColumnFamilyHandle cf = b.objs();
      byte[] key = dbKey(id);

      long referenced = config.currentTimeMicros();
      obj = obj.withReferenced(referenced);

      byte[] serialized =
          serializeObj(
              obj, effectiveIncrementalIndexSizeLimit(), effectiveIndexSegmentSizeLimit(), true);

      db.put(cf, key, serialized);
    } catch (RocksDBException e) {
      throw rocksDbException(e);
    } finally {
      l.unlock();
    }
  }

  @Override
  public void upsertObjs(@Nonnull Obj[] objs) throws ObjTooLargeException {
    for (Obj obj : objs) {
      if (obj != null) {
        upsertObj(obj);
      }
    }
  }

  @Override
  public boolean deleteWithReferenced(@Nonnull Obj obj) {
    ObjId id = obj.id();
    Lock l = repo.objLock(id);
    try {
      RocksDBBackend b = backend;
      TransactionDB db = b.db();
      ColumnFamilyHandle cf = b.objs();
      byte[] key = dbKey(id);

      byte[] bytes = db.get(cf, key);
      if (bytes == null) {
        return false;
      }
      Obj existing = deserializeObj(id, 0L, bytes, null);
      if (!existing.type().equals(obj.type())) {
        return false;
      }
      var referenced = obj.referenced();
      if (existing.referenced() != referenced && referenced != -1L) {
        // -1 is a sentinel for AbstractBasePersistTests.deleteWithReferenced()
        return false;
      }

      db.delete(cf, key);
      return true;
    } catch (RocksDBException e) {
      throw rocksDbException(e);
    } finally {
      l.unlock();
    }
  }

  @Override
  public boolean deleteConditional(@Nonnull UpdateableObj obj) {
    ObjId id = obj.id();
    Lock l = repo.objLock(id);
    try {
      RocksDBBackend b = backend;
      TransactionDB db = b.db();
      ColumnFamilyHandle cf = b.objs();
      byte[] key = dbKey(id);

      byte[] bytes = db.get(cf, key);
      if (bytes == null) {
        return false;
      }
      Obj existing = deserializeObj(id, 0L, bytes, null);
      if (!existing.type().equals(obj.type())) {
        return false;
      }
      UpdateableObj ex = (UpdateableObj) existing;
      if (!ex.versionToken().equals(obj.versionToken())) {
        return false;
      }

      db.delete(cf, key);
      return true;
    } catch (RocksDBException e) {
      throw rocksDbException(e);
    } finally {
      l.unlock();
    }
  }

  @Override
  public boolean updateConditional(@Nonnull UpdateableObj expected, @Nonnull UpdateableObj newValue)
      throws ObjTooLargeException {
    ObjId id = expected.id();
    checkArgument(id != null && id.equals(newValue.id()));
    checkArgument(expected.type().equals(newValue.type()));
    checkArgument(!expected.versionToken().equals(newValue.versionToken()));

    Lock l = repo.objLock(id);
    try {
      RocksDBBackend b = backend;
      TransactionDB db = b.db();
      ColumnFamilyHandle cf = b.objs();
      byte[] key = dbKey(id);

      byte[] obj = db.get(cf, key);
      if (obj == null) {
        return false;
      }
      Obj existing = deserializeObj(id, 0L, obj, null);
      if (!existing.type().equals(expected.type())) {
        return false;
      }
      UpdateableObj ex = (UpdateableObj) existing;
      if (!ex.versionToken().equals(expected.versionToken())) {
        return false;
      }

      long referenced = config.currentTimeMicros();
      byte[] serialized =
          serializeObj(
              newValue.withReferenced(referenced),
              effectiveIncrementalIndexSizeLimit(),
              effectiveIndexSegmentSizeLimit(),
              true);

      db.put(cf, key, serialized);

      return true;
    } catch (RocksDBException e) {
      throw rocksDbException(e);
    } finally {
      l.unlock();
    }
  }

  @Override
  public void erase() {
    backend.eraseRepositories(singleton(config().repositoryId()));
  }

  @Nonnull
  @Override
  public CloseableIterator<Obj> scanAllObjects(@Nonnull Set<ObjType> returnedObjTypes) {
    return new ScanAllObjectsIterator(
        returnedObjTypes.isEmpty() ? x -> true : returnedObjTypes::contains);
  }

  private class ScanAllObjectsIterator extends AbstractIterator<Obj>
      implements CloseableIterator<Obj> {

    private final Predicate<ObjType> filter;

    private final TransactionDB db;
    private final ColumnFamilyHandle cf;
    private final RocksIterator iter;
    private boolean first = true;
    private byte[] lastKey;

    ScanAllObjectsIterator(Predicate<ObjType> filter) {
      this.filter = filter;

      RocksDBBackend b = backend;
      db = b.db();
      cf = b.objs();
      iter = db.newIterator(b.objs());
      iter.seekToFirst();
    }

    @Override
    protected Obj computeNext() {
      while (true) {
        if (!iter.isValid()) {
          return endOfData();
        }

        if (first) {
          first = false;
        } else {
          iter.next();
        }

        byte[] k = iter.key();
        if (lastKey != null && Arrays.equals(lastKey, k)) {
          // RocksDB sometimes tends to return the same key twice
          continue;
        }
        lastKey = k;

        ByteString key = ByteString.copyFrom(k);
        if (!key.startsWith(keyPrefix)) {
          continue;
        }

        byte[] obj;
        try {
          obj = db.get(cf, k);
        } catch (RocksDBException e) {
          throw rocksDbException(e);
        }
        if (obj == null) {
          continue;
        }

        ObjId id = deserializeObjId(key.substring(keyPrefix.size()));
        Obj o = deserializeObj(id, 0L, obj, null);

        if (filter.test(o.type())) {
          return o;
        }
      }
    }

    @Override
    public void close() {
      iter.close();
    }
  }
}

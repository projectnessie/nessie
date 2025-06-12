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
package org.projectnessie.versioned.storage.jdbc2;

import static java.util.Collections.singleton;

import jakarta.annotation.Nonnull;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Set;
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
import org.projectnessie.versioned.storage.common.persist.Reference;

class Jdbc2Persist extends AbstractJdbc2Persist {

  private final Jdbc2Backend backend;

  Jdbc2Persist(Jdbc2Backend backend, StoreConfig config) {
    super(backend.databaseSpecific(), backend.fetchSize(), config);
    this.backend = backend;
  }

  @FunctionalInterface
  interface SQLRunnableException<R, E extends Exception> {
    R run(Connection conn) throws E;
  }

  @FunctionalInterface
  interface SQLRunnableExceptions<R, E1 extends Exception, E2 extends Exception> {
    R run(Connection conn) throws E1, E2;
  }

  @FunctionalInterface
  interface SQLRunnableVoid {
    void run(Connection conn);
  }

  private void withConnectionVoid(SQLRunnableVoid runnable) {
    withConnectionException(
        false,
        conn -> {
          runnable.run(conn);
          return null;
        });
  }

  private <R, E extends Exception> R withConnectionException(
      boolean readOnly, SQLRunnableException<R, E> runnable) throws E {
    try (Connection conn = backend.borrowConnection()) {
      boolean ok = false;
      R r;
      try {
        r = runnable.run(conn);
        ok = true;
      } finally {
        if (!readOnly) {
          if (ok) {
            conn.commit();
          } else {
            conn.rollback();
          }
        }
      }
      return r;
    } catch (SQLException e) {
      throw unhandledSQLException(e);
    }
  }

  private <R, E1 extends Exception, E2 extends Exception> R withConnectionExceptions(
      SQLRunnableExceptions<R, E1, E2> runnable) throws E1, E2 {
    try (Connection conn = backend.borrowConnection()) {
      boolean ok = false;
      R r;
      try {
        r = runnable.run(conn);
        ok = true;
      } finally {
        if (ok) {
          conn.commit();
        } else {
          conn.rollback();
        }
      }
      return r;
    } catch (SQLException e) {
      throw unhandledSQLException(e);
    }
  }

  @Override
  public Reference fetchReference(@Nonnull String name) {
    return withConnectionException(true, conn -> super.findReference(conn, name));
  }

  @Override
  @Nonnull
  public Reference[] fetchReferences(@Nonnull String[] names) {
    return withConnectionException(true, conn -> super.findReferences(conn, names));
  }

  @Override
  @Nonnull
  public Reference addReference(@Nonnull Reference reference) throws RefAlreadyExistsException {
    return withConnectionException(false, conn -> super.addReference(conn, reference));
  }

  @Override
  @Nonnull
  public Reference markReferenceAsDeleted(@Nonnull Reference reference)
      throws RefNotFoundException, RefConditionFailedException {
    return withConnectionExceptions(
        (SQLRunnableExceptions<Reference, RefNotFoundException, RefConditionFailedException>)
            conn -> super.markReferenceAsDeleted(conn, reference));
  }

  @Override
  public void purgeReference(@Nonnull Reference reference)
      throws RefNotFoundException, RefConditionFailedException {
    withConnectionExceptions(
        (SQLRunnableExceptions<Reference, RefNotFoundException, RefConditionFailedException>)
            conn -> {
              super.purgeReference(conn, reference);
              return null;
            });
  }

  @Override
  @Nonnull
  public Reference updateReferencePointer(@Nonnull Reference reference, @Nonnull ObjId newPointer)
      throws RefNotFoundException, RefConditionFailedException {
    return withConnectionExceptions(
        (SQLRunnableExceptions<Reference, RefNotFoundException, RefConditionFailedException>)
            conn -> super.updateReferencePointer(conn, reference, newPointer));
  }

  @Override
  @Nonnull
  public <T extends Obj> T fetchTypedObj(
      @Nonnull ObjId id, ObjType type, @Nonnull Class<T> typeClass) throws ObjNotFoundException {
    return withConnectionException(true, conn -> super.fetchTypedObj(conn, id, type, typeClass));
  }

  @Override
  @Nonnull
  public ObjType fetchObjType(@Nonnull ObjId id) throws ObjNotFoundException {
    return withConnectionException(true, conn -> super.fetchObjType(conn, id));
  }

  @Override
  public <T extends Obj> T[] fetchTypedObjsIfExist(
      @Nonnull ObjId[] ids, ObjType type, @Nonnull Class<T> typeClass) {
    return withConnectionException(
        true, conn -> super.fetchTypedObjsIfExist(conn, ids, type, typeClass));
  }

  @Override
  public boolean storeObj(@Nonnull Obj obj, boolean ignoreSoftSizeRestrictions)
      throws ObjTooLargeException {
    return withConnectionException(
        false, conn -> super.storeObj(conn, obj, ignoreSoftSizeRestrictions));
  }

  @Override
  @Nonnull
  public boolean[] storeObjs(@Nonnull Obj[] objs) throws ObjTooLargeException {
    return withConnectionException(false, conn -> super.storeObjs(conn, objs));
  }

  @Override
  public void deleteObj(@Nonnull ObjId id) {
    withConnectionVoid(conn -> super.deleteObj(conn, id));
  }

  @Override
  public void deleteObjs(@Nonnull ObjId[] ids) {
    withConnectionVoid(conn -> super.deleteObjs(conn, ids));
  }

  @Override
  public void upsertObj(@Nonnull Obj obj) throws ObjTooLargeException {
    withConnectionException(false, conn -> super.updateObj(conn, obj));
  }

  @Override
  public void upsertObjs(@Nonnull Obj[] objs) throws ObjTooLargeException {
    withConnectionException(false, conn -> super.updateObjs(conn, objs));
  }

  @Override
  public boolean deleteWithReferenced(@Nonnull Obj obj) {
    return withConnectionException(false, conn -> super.deleteWithReferenced(conn, obj));
  }

  @Override
  public boolean deleteConditional(@Nonnull UpdateableObj obj) {
    return withConnectionException(false, conn -> super.deleteConditional(conn, obj));
  }

  @Override
  public boolean updateConditional(@Nonnull UpdateableObj expected, @Nonnull UpdateableObj newValue)
      throws ObjTooLargeException {
    return withConnectionException(
        false, conn -> super.updateConditional(conn, expected, newValue));
  }

  @Override
  public void erase() {
    backend.eraseRepositories(singleton(config().repositoryId()));
  }

  @Nonnull
  @Override
  public CloseableIterator<Obj> scanAllObjects(@Nonnull Set<ObjType> returnedObjTypes) {
    try {
      return super.scanAllObjects(backend.borrowConnection(), returnedObjTypes);
    } catch (SQLException e) {
      throw unhandledSQLException(e);
    }
  }
}

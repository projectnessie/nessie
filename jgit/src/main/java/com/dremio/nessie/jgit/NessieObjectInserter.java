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

import com.dremio.nessie.error.NessieConflictException;
import java.io.IOException;
import java.io.InputStream;

import org.eclipse.jgit.internal.storage.dfs.DfsInserter;
import org.eclipse.jgit.internal.storage.dfs.DfsObjDatabase;
import org.eclipse.jgit.lib.Constants;
import org.eclipse.jgit.lib.ObjectId;
import org.eclipse.jgit.util.IO;

public class NessieObjectInserter extends DfsInserter {

  private final NessieObjDatabase db;

  /**
   * Initialize a new inserter.
   *
   * @param db database the inserter writes to.
   */
  protected NessieObjectInserter(DfsObjDatabase db) {
    super(db);
    this.db = (NessieObjDatabase) db;
  }

  @Override
  public ObjectId insert(int type, byte[] data, int off, int len) {
    ObjectId id = idFor(type, data, off, len);
    if (db.has(id, true)) {
      return id;
    }

    byte[] buf = new byte[len];
    System.arraycopy(data, off, buf, 0, len);
    if (type == Constants.OBJ_REF_DELTA) {
      db.put(id, type, buf);
    } else {
      db.putAll(id, type, buf);
    }
    return id;
  }

  @Override
  public ObjectId insert(int type, long len, InputStream in) throws IOException {
    byte[] buf = insertBuffer(len);
    if (len <= buf.length) {
      IO.readFully(in, buf, 0, (int) len);
      return insert(type, buf, 0, (int) len);
    }
    throw new NessieConflictException(null,
                                      "unable to insert buf of type " + type + " with value "
                                      + new String(buf));
  }

  @Override
  public ObjectId insert(int type, byte[] data) {
    return insert(type, data, 0, data.length);
  }

  @Override
  public void flush() {
    db.flush();
  }

  private byte[] insertBuffer(long len) {
    byte[] buf = buffer();
    if (len <= buf.length) {
      return buf;
    }
    if (len < db.getReaderOptions().getStreamFileThreshold()) {
      try {
        return new byte[(int) len];
      } catch (OutOfMemoryError noMem) {
        return buf;
      }
    }
    return buf;
  }
}

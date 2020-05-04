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

import java.io.IOException;
import org.eclipse.jgit.internal.storage.dfs.DfsObjDatabase;
import org.eclipse.jgit.internal.storage.dfs.DfsReader;
import org.eclipse.jgit.lib.AnyObjectId;
import org.eclipse.jgit.lib.ObjectLoader;

public class NessieObjReader extends DfsReader {

  private final NessieObjDatabase db;

  /**
   * Initialize a new DfsReader.
   *
   * @param db parent DfsObjDatabase.
   */
  protected NessieObjReader(DfsObjDatabase db) {
    super(db);
    this.db = (NessieObjDatabase) db;
  }

  @Override
  public boolean has(AnyObjectId objectId) throws IOException {
    return db.get(objectId, -1) != null;
  }

  @Override
  public ObjectLoader open(AnyObjectId objectId, int typeHint) throws IOException {
    return db.get(objectId, typeHint);
  }
}

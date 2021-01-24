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
package com.dremio.nessie.versioned.gc;

import java.util.stream.Stream;

import com.dremio.nessie.versioned.AssetKey;

public interface AssetKeyReader {

  enum AssetKeyType {
    TABLE(true),
    ICEBERG_MANIFEST(false),
    ICEBERG_MANIFEST_LIST(false),
    ICEBERG_METADATA(false),
    DATA_FILE(false);

    private final boolean recursive;

    AssetKeyType(boolean recursive) {
      this.recursive = recursive;
    }

    public boolean isRecursive() {
      return recursive;
    }
  }

  Stream<AssetKey> getKeys();
}

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
package com.dremio.nessie.versioned.gc.assets;

/**
 * possible Asset Key types. Here recursiveDelete is applicable only to filesystem Asset Keys.
 */
public enum AssetKeyType {
  TABLE(true),
  ICEBERG_MANIFEST(false),
  ICEBERG_MANIFEST_LIST(false),
  ICEBERG_METADATA(false),
  DATA_FILE(false);

  private final boolean recursiveDelete;

  AssetKeyType(boolean recursive) {
    this.recursiveDelete = recursive;
  }

  public boolean isRecursiveDelete() {
    return recursiveDelete;
  }
}

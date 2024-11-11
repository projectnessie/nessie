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
package org.apache.iceberg;

import com.google.common.collect.ImmutableList;
import java.util.Map;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;

public final class ManifestReaderUtil {

  private ManifestReaderUtil() {}

  public static CloseableIterable<String> readPathsFromManifest(
      ManifestFile manifest, Map<Integer, PartitionSpec> specsById, FileIO io) {
    // entry.file() is package private. Hence, this Util class under iceberg package.
    return CloseableIterable.transform(
        ManifestFiles.open(manifest, io, specsById)
            .select(ImmutableList.of("file_path"))
            .liveEntries(),
        entry -> entry.file().location());
  }
}

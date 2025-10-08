/*
 * Copyright (C) 2025 Dremio
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
package org.projectnessie.catalog.service.files;

import java.io.IOException;
import java.io.InputStream;
import java.util.zip.GZIPInputStream;
import org.projectnessie.catalog.files.api.ObjectIO;
import org.projectnessie.storage.uri.StorageUri;

public final class MetadataUtil {
  private MetadataUtil() {}

  public static InputStream readMetadata(ObjectIO io, StorageUri uri) throws IOException {
    boolean compressed =
        uri.requiredPath().endsWith(".gz") || uri.requiredPath().endsWith(".gz.metadata.json");
    final InputStream input = io.readObject(uri);
    if (compressed) {
      return new GZIPInputStream(input);
    }
    return input;
  }
}

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
package org.projectnessie.gc.iceberg.files;

import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;
import org.projectnessie.gc.files.FileDeleter;
import org.projectnessie.gc.files.FileReference;
import org.projectnessie.gc.files.FilesLister;
import org.projectnessie.gc.files.tests.AbstractFiles;
import org.projectnessie.storage.uri.StorageUri;

public class TestIcebergFilesLocal extends AbstractFiles {

  @TempDir Path path;

  private IcebergFiles hadoop;

  @BeforeEach
  void setUp() {
    hadoop = IcebergFiles.builder().build();
  }

  @AfterEach
  void tearDown() {
    if (hadoop != null) {
      try {
        hadoop.close();
      } finally {
        hadoop = null;
      }
    }
  }

  @Override
  protected StorageUri baseUri() {
    return StorageUri.of(path.toUri());
  }

  @Override
  protected List<FileReference> prepareFiles(int numFiles) {
    return prepareLocalFiles(path, numFiles);
  }

  @Override
  protected FilesLister lister() {
    return hadoop;
  }

  @Override
  protected FileDeleter deleter() {
    return hadoop;
  }
}

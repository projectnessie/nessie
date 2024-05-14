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
package org.projectnessie.gc.files.local;

import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;
import org.projectnessie.gc.files.FileDeleter;
import org.projectnessie.gc.files.FileReference;
import org.projectnessie.gc.files.FilesLister;
import org.projectnessie.gc.files.tests.AbstractFiles;
import org.projectnessie.storage.uri.StorageUri;

public class TestLocalFiles extends AbstractFiles {

  @TempDir Path path;

  private LocalFiles local;

  @BeforeEach
  void setUp() {
    local = new LocalFiles();
  }

  @Override
  protected FilesLister lister() {
    return local;
  }

  @Override
  protected FileDeleter deleter() {
    return local;
  }

  @Override
  protected List<FileReference> prepareFiles(int numFiles) {
    return prepareLocalFiles(path, numFiles);
  }

  @Override
  protected StorageUri baseUri() {
    return StorageUri.of(path.toUri());
  }
}

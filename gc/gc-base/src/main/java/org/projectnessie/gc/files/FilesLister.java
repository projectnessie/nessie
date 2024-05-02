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
package org.projectnessie.gc.files;

import com.google.errorprone.annotations.MustBeClosed;
import java.util.stream.Stream;
import org.projectnessie.storage.uri.StorageUri;

/** Function to list files recursively. */
public interface FilesLister {

  /**
   * Returns a {@link Stream} of {@link FileReference} contained in the given {@code path}. The
   * returned {@link FileReference}s have their {@link FileReference#base()} set to the {@code path}
   * argument.
   */
  @MustBeClosed
  Stream<FileReference> listRecursively(StorageUri path) throws NessieFileIOException;
}

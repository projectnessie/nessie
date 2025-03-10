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
package org.projectnessie.versioned.transfer.files;

import jakarta.annotation.Nonnull;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Path;

public interface ExportFileSupplier extends AutoCloseable {

  @Nonnull
  Path getTargetPath();

  void preValidate() throws IOException;

  @Nonnull
  OutputStream newFileOutput(@Nonnull String fileName) throws IOException;

  long fixMaxFileSize(long userProvidedMaxFileSize);
}

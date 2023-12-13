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
package org.projectnessie.versioned.transfer;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import org.projectnessie.versioned.transfer.serialize.TransferTypes.ExportMeta;

@FunctionalInterface
public interface ProgressListener {
  default void progress(@Nonnull ProgressEvent type) {
    progress(type, null);
  }

  /**
   * Reports a progress event. The {@code exportMeta} parameter is only valid for {@link
   * ProgressEvent#END_META}.
   */
  void progress(@Nonnull ProgressEvent type, @Nullable ExportMeta exportMeta);
}

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

import org.immutables.value.Value;
import org.projectnessie.versioned.transfer.serialize.TransferTypes.ExportMeta;
import org.projectnessie.versioned.transfer.serialize.TransferTypes.HeadsAndForks;

/** The result of a {@link NessieImporter#importNessieRepository()} operation. */
@Value.Immutable
public interface ImportResult {

  /** Export meta information from the export archive. */
  ExportMeta exportMeta();

  /**
   * Heads and fork-points generated during the export.
   *
   * <p>Note: the HEADS of the named references in an export may not match the heads in {@link
   * HeadsAndForks} when commits happened while the export has been created.
   */
  HeadsAndForks headsAndForks();

  /** Number of commits that have been imported. */
  long importedCommitCount();

  /** Number of generic objects that have been imported. */
  long importedGenericCount();

  /** Number of references that have been imported. */
  long importedReferenceCount();
}

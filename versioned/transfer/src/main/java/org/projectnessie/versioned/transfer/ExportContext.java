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

import java.io.IOException;
import org.projectnessie.versioned.transfer.files.ExportFileSupplier;
import org.projectnessie.versioned.transfer.serialize.TransferTypes.Commit;
import org.projectnessie.versioned.transfer.serialize.TransferTypes.ExportMeta;
import org.projectnessie.versioned.transfer.serialize.TransferTypes.Ref;
import org.projectnessie.versioned.transfer.serialize.TransferTypes.RelatedObj;

final class ExportContext {

  private final ExportMeta.Builder exportMeta;

  final SizeLimitedOutput namedReferenceOutput;
  final SizeLimitedOutput commitOutput;
  final SizeLimitedOutput genericOutput;

  ExportContext(
      ExportFileSupplier exportFiles, NessieExporter exporter, ExportMeta.Builder exportMeta) {
    this.exportMeta = exportMeta;
    namedReferenceOutput =
        new SizeLimitedOutput(
            exportFiles,
            exporter,
            NessieExporter.NAMED_REFS_PREFIX,
            exportMeta::addNamedReferencesFiles,
            exportMeta::setNamedReferencesCount);
    commitOutput =
        new SizeLimitedOutput(
            exportFiles,
            exporter,
            NessieExporter.COMMITS_PREFIX,
            exportMeta::addCommitsFiles,
            exportMeta::setCommitCount);
    genericOutput =
        new SizeLimitedOutput(
            exportFiles,
            exporter,
            NessieExporter.CUSTOM_PREFIX,
            exportMeta::addGenericObjFiles,
            exportMeta::setGenericObjCount);
  }

  public void writeRef(Ref ref) {
    namedReferenceOutput.writeEntity(ref);
  }

  void writeCommit(Commit commit) {
    commitOutput.writeEntity(commit);
  }

  void writeGeneric(RelatedObj custom) {
    genericOutput.writeEntity(custom);
  }

  ExportMeta finish() throws IOException {
    namedReferenceOutput.finish();
    commitOutput.finish();
    genericOutput.finish();
    return exportMeta.build();
  }

  void closeSilently() {
    namedReferenceOutput.closeSilently();
    commitOutput.closeSilently();
    genericOutput.closeSilently();
  }
}

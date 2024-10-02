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

import static org.projectnessie.versioned.storage.common.objtypes.StandardObjType.UNIQUE;
import static org.projectnessie.versioned.transfer.ExportImportConstants.EXPORT_METADATA;
import static org.projectnessie.versioned.transfer.ExportImportConstants.HEADS_AND_FORKS;
import static org.projectnessie.versioned.transfer.ExportImportConstants.REPOSITORY_DESCRIPTION;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import org.agrona.collections.ObjectHashSet;
import org.projectnessie.api.NessieVersion;
import org.projectnessie.nessie.relocated.protobuf.UnsafeByteOperations;
import org.projectnessie.versioned.storage.common.logic.RepositoryDescription;
import org.projectnessie.versioned.storage.common.objtypes.CustomObjType;
import org.projectnessie.versioned.storage.common.objtypes.UniqueIdObj;
import org.projectnessie.versioned.storage.common.objtypes.UpdateableObj;
import org.projectnessie.versioned.storage.common.persist.Obj;
import org.projectnessie.versioned.storage.common.persist.ObjId;
import org.projectnessie.versioned.storage.common.persist.ObjType;
import org.projectnessie.versioned.storage.serialize.SmileSerialization;
import org.projectnessie.versioned.transfer.files.ExportFileSupplier;
import org.projectnessie.versioned.transfer.related.CompositeTransferRelatedObjects;
import org.projectnessie.versioned.transfer.related.TransferRelatedObjects;
import org.projectnessie.versioned.transfer.serialize.TransferTypes;
import org.projectnessie.versioned.transfer.serialize.TransferTypes.ExportMeta;
import org.projectnessie.versioned.transfer.serialize.TransferTypes.ExportVersion;
import org.projectnessie.versioned.transfer.serialize.TransferTypes.HeadsAndForks;
import org.projectnessie.versioned.transfer.serialize.TransferTypes.RelatedObj;
import org.projectnessie.versioned.transfer.serialize.TransferTypes.RepositoryDescriptionProto;

abstract class ExportCommon {
  final ExportFileSupplier exportFiles;
  final NessieExporter exporter;
  final TransferRelatedObjects transferRelatedObjects;
  final ExportVersion exportVersion;

  private Batcher<Obj> genericObjBatcher;

  ExportCommon(
      ExportFileSupplier exportFiles, NessieExporter exporter, ExportVersion exportVersion) {
    this.exportFiles = exportFiles;
    this.exporter = exporter;

    this.exportVersion = verifyExportVersion(exportVersion);

    /*
     * One "related" object might be referenced by multiple content objects or commits or references.
     * This implementation avoid exporting the same "related" object more than once. This set of
     * {@link ObjId}s is unbounded, like the collections in {@link IdentifyHeadsAndForkPoints}.
     */
    ObjectHashSet<ObjId> seen = new ObjectHashSet<>();
    this.transferRelatedObjects =
        CompositeTransferRelatedObjects.createCompositeTransferRelatedObjects(
            exporter.genericObjectResolvers(), seen::add);
  }

  private static ExportVersion verifyExportVersion(ExportVersion exportVersion) {
    if (exportVersion == null) {
      exportVersion = ExportVersion.UNRECOGNIZED;
    }
    switch (exportVersion) {
      case V1:
        throw new IllegalArgumentException(
            "Cannot export using export-version " + exportVersion.getNumber());
      case V2:
      case V3:
        break;
      default:
        throw new IllegalArgumentException(
            "Unimplemented export-version " + exportVersion.getNumber());
    }

    return exportVersion;
  }

  ExportMeta exportRepo() throws IOException {
    ExportContext exportContext = createExportContext(getExportVersion());
    try {
      // Generic objects are written by multiple export tasks, so we need to open (and close) its
      // `Batcher` instance here.
      try (Batcher<Obj> genericObjBatcher =
          new Batcher<>(exporter.commitBatchSize(), objs -> mapGenericObjs(objs, exportContext))) {
        this.genericObjBatcher = genericObjBatcher;

        exporter.progressListener().progress(ProgressEvent.STARTED);
        prepare(exportContext);

        exporter.progressListener().progress(ProgressEvent.START_COMMITS);
        HeadsAndForks headsAndForks = exportCommits(exportContext);
        exportContext.commitOutput.finishCurrentFile();
        writeHeadsAndForks(headsAndForks);
        exporter.progressListener().progress(ProgressEvent.END_COMMITS);

        exporter.progressListener().progress(ProgressEvent.START_NAMED_REFERENCES);
        exportReferences(exportContext);
        exportContext.namedReferenceOutput.finishCurrentFile();
        exporter.progressListener().progress(ProgressEvent.END_NAMED_REFERENCES);

        exporter.progressListener().progress(ProgressEvent.START_META);
        writeRepositoryInformation();

      } finally {
        this.genericObjBatcher = null;
      }

      // Must close the `Batcher` before `ExportContext.finish()` to have the right values in
      // `ExportMeta`.

      ExportMeta meta = exportContext.finish();
      writeExportMeta(meta);
      exporter.progressListener().progress(ProgressEvent.END_META, meta);

      exporter.progressListener().progress(ProgressEvent.FINISHED);

      return meta;
    } finally {
      // Ensure that all output streams are closed.
      exportContext.closeSilently();
    }
  }

  final long currentTimestampMillis() {
    return exporter.clock().millis();
  }

  ExportContext createExportContext(ExportVersion exportVersion) {
    return new ExportContext(
        exportFiles,
        exporter,
        ExportMeta.newBuilder()
            .setNessieVersion(NessieVersion.NESSIE_VERSION)
            .setCreatedMillisEpoch(currentTimestampMillis())
            .setVersion(exportVersion));
  }

  ExportVersion getExportVersion() {
    return exportVersion;
  }

  abstract void prepare(ExportContext exportContext);

  abstract void exportReferences(ExportContext exportContext);

  abstract HeadsAndForks exportCommits(ExportContext exportContext);

  void writeRepositoryInformation() throws IOException {
    handleGenericObjs(transferRelatedObjects.repositoryRelatedObjects());

    RepositoryDescription repositoryDescription =
        exporter.repositoryLogic().fetchRepositoryDescription();
    if (repositoryDescription != null) {
      writeRepositoryDescription(
          TransferTypes.RepositoryDescriptionProto.newBuilder()
              .putAllProperties(repositoryDescription.properties())
              .setRepositoryId(exporter.persist().config().repositoryId())
              .setRepositoryCreatedTimestampMillis(
                  repositoryDescription.repositoryCreatedTime().toEpochMilli())
              .setOldestCommitTimestampMillis(
                  repositoryDescription.oldestPossibleCommitTime().toEpochMilli())
              .setDefaultBranchName(repositoryDescription.defaultBranchName())
              .build());
    }
  }

  void writeExportMeta(ExportMeta meta) throws IOException {
    try (OutputStream output = exportFiles.newFileOutput(EXPORT_METADATA)) {
      meta.writeTo(output);
    }
  }

  void writeHeadsAndForks(HeadsAndForks hf) throws IOException {
    try (OutputStream output = exportFiles.newFileOutput(HEADS_AND_FORKS)) {
      hf.writeTo(output);
    }
  }

  void writeRepositoryDescription(RepositoryDescriptionProto repositoryDescription)
      throws IOException {
    try (OutputStream output = exportFiles.newFileOutput(REPOSITORY_DESCRIPTION)) {
      repositoryDescription.writeTo(output);
    }
  }

  void handleGenericObjs(Set<ObjId> ids) {
    if (ids.isEmpty()) {
      return;
    }

    ObjId[] idArray = ids.toArray(ObjId[]::new);
    Obj[] objs = exporter.persist().fetchObjsIfExist(idArray);
    Arrays.stream(objs).filter(Objects::nonNull).forEach(genericObjBatcher::add);
  }

  private void mapGenericObjs(List<Obj> objs, ExportContext exportContext) {
    for (Obj o : objs) {
      RelatedObj custom = mapGenericObj(o);
      exportContext.writeGeneric(custom);
      // use the same progress value than commits
      exporter.progressListener().progress(ProgressEvent.GENERIC_WRITTEN);
    }
  }

  private RelatedObj mapGenericObj(Obj obj) {
    RelatedObj.Builder genericObj = RelatedObj.newBuilder().setId(obj.id().asBytes());
    ObjType objType = obj.type();
    if (objType.equals(UNIQUE)) {
      UniqueIdObj uniqueIdObj = (UniqueIdObj) obj;
      genericObj
          .setTypeName(UNIQUE.shortName())
          .setSpace(uniqueIdObj.space())
          .setData(uniqueIdObj.value());
    } else if (objType instanceof CustomObjType) {
      // 'obj' can be a 'GenericObj' or, if the actual `ObjType` implementations are on the
      // classpath, the "real" instance, so we need to handle both cases.
      byte[] serialized =
          SmileSerialization.serializeObj(
              obj, compression -> genericObj.setCompression(compression.valueString()));
      genericObj
          .setTypeName(objType.shortName())
          .setEncoding(TransferTypes.Encoding.Smile)
          .setData(UnsafeByteOperations.unsafeWrap(serialized));
      if (obj instanceof UpdateableObj) {
        UpdateableObj updateableObj = (UpdateableObj) obj;
        String versionToken = updateableObj.versionToken();
        if (versionToken != null) {
          genericObj.setVersionToken(versionToken);
        }
      }
    } else {
      throw new IllegalArgumentException("Unhandled object type " + objType.name());
    }
    return genericObj.build();
  }
}

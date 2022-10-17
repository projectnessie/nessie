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
package org.projectnessie.gc.iceberg.inttest;

import java.time.Instant;
import java.util.UUID;
import org.projectnessie.gc.contents.LiveContentSet;
import org.projectnessie.gc.contents.LiveContentSetNotFoundException;
import org.projectnessie.gc.contents.LiveContentSetsRepository;
import org.projectnessie.gc.contents.spi.PersistenceSpi;
import org.projectnessie.gc.expire.Expire;
import org.projectnessie.gc.expire.ExpireParameters;
import org.projectnessie.gc.expire.local.DefaultLocalExpire;
import org.projectnessie.gc.files.DeleteSummary;
import org.projectnessie.gc.iceberg.IcebergContentToContentReference;
import org.projectnessie.gc.iceberg.IcebergContentToFiles;
import org.projectnessie.gc.iceberg.IcebergContentTypeFilter;
import org.projectnessie.gc.iceberg.files.IcebergFiles;
import org.projectnessie.gc.identify.IdentifyLiveContents;
import org.projectnessie.gc.identify.PerRefCutoffPolicySupplier;
import org.projectnessie.gc.repository.RepositoryConnector;

final class Util {
  private Util() {}

  static LiveContentSet identifyLiveContents(
      PersistenceSpi persistenceSpi,
      PerRefCutoffPolicySupplier cutOffPolicySupplier,
      RepositoryConnector repositoryConnector)
      throws LiveContentSetNotFoundException {
    IdentifyLiveContents identify =
        IdentifyLiveContents.builder()
            .liveContentSetsRepository(
                LiveContentSetsRepository.builder().persistenceSpi(persistenceSpi).build())
            .contentTypeFilter(IcebergContentTypeFilter.INSTANCE)
            .cutOffPolicySupplier(cutOffPolicySupplier)
            .repositoryConnector(repositoryConnector)
            .contentToContentReference(IcebergContentToContentReference.INSTANCE)
            .build();

    UUID liveSetId = identify.identifyLiveContents();

    return persistenceSpi.getLiveContentSet(liveSetId);
  }

  static DeleteSummary expire(
      IcebergFiles icebergFiles, LiveContentSet liveContentSet, Instant maxFileModificationTime) {
    ExpireParameters expireParameters =
        ExpireParameters.builder()
            .fileDeleter(icebergFiles)
            .filesLister(icebergFiles)
            .contentToFiles(
                IcebergContentToFiles.builder().io(icebergFiles.resolvingFileIO()).build())
            .liveContentSet(liveContentSet)
            .maxFileModificationTime(maxFileModificationTime)
            .build();

    Expire expire = DefaultLocalExpire.builder().expireParameters(expireParameters).build();
    return expire.expire();
  }
}

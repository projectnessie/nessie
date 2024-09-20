/*
 * Copyright (C) 2024 Dremio
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
package org.projectnessie.catalog.service.api;

import jakarta.annotation.Nullable;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.projectnessie.api.v2.params.ParsedReference;
import org.projectnessie.catalog.model.snapshot.NessieEntitySnapshot;
import org.projectnessie.catalog.service.config.WarehouseConfig;
import org.projectnessie.error.BaseNessieClientServerException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.Reference;
import org.projectnessie.services.authz.ApiContext;
import org.projectnessie.storage.uri.StorageUri;
import org.projectnessie.versioned.RequestMeta;

public interface CatalogService {

  /**
   * Retrieves table or view snapshot related information.
   *
   * @param reqParams Parameters holding the Nessie reference specification, snapshot format and
   *     more.
   * @param key content key of the table or view
   * @param expectedType The expected content-type.
   * @param requestMeta additional information for access checks
   * @param apiContext
   * @return The response is either a response object or callback to produce the result. The latter
   *     is useful to return results that are quite big, for example Iceberg manifest lists or
   *     manifest files.
   */
  CompletionStage<SnapshotResponse> retrieveSnapshot(
      SnapshotReqParams reqParams,
      ContentKey key,
      @Nullable Content.Type expectedType,
      RequestMeta requestMeta,
      ApiContext apiContext)
      throws NessieNotFoundException;

  Stream<Supplier<CompletionStage<SnapshotResponse>>> retrieveSnapshots(
      SnapshotReqParams reqParams,
      List<ContentKey> keys,
      Consumer<Reference> effectiveReferenceConsumer,
      RequestMeta requestMeta,
      ApiContext apiContext)
      throws NessieNotFoundException;

  CompletionStage<Stream<SnapshotResponse>> commit(
      ParsedReference reference,
      CatalogCommit commit,
      SnapshotReqParams reqParams,
      Function<String, CommitMeta> commitMetaBuilder,
      String apiRequest,
      ApiContext apiContext)
      throws BaseNessieClientServerException;

  interface CatalogUriResolver {
    URI icebergSnapshot(
        Reference effectiveReference, ContentKey key, NessieEntitySnapshot<?> snapshot);
  }

  Optional<String> validateStorageLocation(String location);

  StorageUri locationForEntity(
      WarehouseConfig warehouse,
      ContentKey contentKey,
      Content.Type contentType,
      ApiContext apiContext,
      String refName,
      String hash)
      throws NessieNotFoundException;

  StorageUri locationForEntity(
      WarehouseConfig warehouse,
      ContentKey contentKey,
      List<ContentKey> keysInOrder,
      Map<ContentKey, Content> contentsMap);
}

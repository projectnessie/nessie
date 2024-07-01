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
package org.projectnessie.catalog.service.impl;

import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.InstanceOfAssertFactories.STRING;
import static org.projectnessie.api.v2.params.ParsedReference.parsedReference;
import static org.projectnessie.catalog.service.api.SnapshotReqParams.forSnapshotHttpReq;
import static org.projectnessie.model.CommitMeta.fromMessage;
import static org.projectnessie.model.Content.Type.ICEBERG_TABLE;
import static org.projectnessie.services.authz.Check.CheckType.COMMIT_CHANGE_AGAINST_REFERENCE;
import static org.projectnessie.services.authz.Check.CheckType.READ_ENTITY_VALUE;
import static org.projectnessie.services.authz.Check.CheckType.UPDATE_ENTITY;
import static org.projectnessie.services.authz.Check.CheckType.VIEW_REFERENCE;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.assertj.core.api.AbstractThrowableAssert;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.projectnessie.api.v2.params.ParsedReference;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergJson;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergTableMetadata;
import org.projectnessie.catalog.formats.iceberg.nessie.NessieModelIceberg;
import org.projectnessie.catalog.model.snapshot.NessieTableSnapshot;
import org.projectnessie.catalog.service.api.CatalogCommit;
import org.projectnessie.catalog.service.api.CatalogService;
import org.projectnessie.catalog.service.api.SnapshotReqParams;
import org.projectnessie.catalog.service.api.SnapshotResponse;
import org.projectnessie.error.NessieReferenceConflictException;
import org.projectnessie.model.Branch;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.IcebergView;
import org.projectnessie.model.Operation;
import org.projectnessie.model.Reference;
import org.projectnessie.objectstoragemock.Bucket;
import org.projectnessie.objectstoragemock.MockObject;
import org.projectnessie.services.authz.AbstractBatchAccessChecker;
import org.projectnessie.services.authz.AccessCheckException;
import org.projectnessie.services.authz.Check;
import org.projectnessie.services.authz.Check.CheckType;
import org.projectnessie.storage.uri.StorageUri;
import software.amazon.awssdk.services.s3.model.S3Exception;

public class TestCatalogServiceImpl extends AbstractCatalogService {

  /** Check that a non-modifying catalog-commit is a no-op. */
  @Test
  public void cleanupAfterNessieCommitFailure() throws Exception {
    Reference main = api.getReference().refName("main").get();
    ContentKey key1 = ContentKey.of("mytable");
    ContentKey key2 = ContentKey.of("othertable");

    AtomicReference<List<String>> storedLocations = new AtomicReference<>(new ArrayList<>());

    interceptingBucket.setUpdater(
        (k, m) -> {
          List<String> l = storedLocations.get();
          l.add(k);
          storedLocations.set(l);
          return Optional.empty();
        });

    api.commitMultipleOperations()
        .branch((Branch) main)
        .commitMeta(fromMessage("break next commit"))
        .operation(Operation.Put.of(key1, IcebergView.of("meta", 1, 2)))
        .commitWithResponse();

    soft.assertThatThrownBy(() -> commitMultiple(main, key1, key2))
        .isInstanceOf(ExecutionException.class)
        .cause()
        .isInstanceOf(RuntimeException.class)
        .hasCauseInstanceOf(NessieReferenceConflictException.class);
    soft.assertThat(storedLocations.get()).hasSize(2);
    soft.assertThat(heapStorageBucket.objects()).isEmpty();
  }

  @ParameterizedTest
  @ValueSource(ints = {0, 1, 2})
  public void cleanupAfterObjectIoFailure(int after) throws Exception {
    Reference main = api.getReference().refName("main").get();
    ContentKey key1 = ContentKey.of("mytable1");
    ContentKey key2 = ContentKey.of("mytable2");
    ContentKey key3 = ContentKey.of("mytable3");
    ContentKey key4 = ContentKey.of("mytable4");

    AtomicReference<List<String>> storedLocations = new AtomicReference<>(new ArrayList<>());
    AtomicReference<List<String>> failedLocations = new AtomicReference<>(new ArrayList<>());

    interceptingBucket.setUpdater(
        (k, m) -> {
          List<String> l = storedLocations.get();
          if (l.size() == after) {
            l = failedLocations.get();
            l.add(k);
            failedLocations.set(l);
            return Optional.of(
                new Bucket.ObjectUpdater() {
                  @Override
                  public Bucket.ObjectUpdater append(long position, InputStream data) {
                    return this;
                  }

                  @Override
                  public Bucket.ObjectUpdater flush() {
                    return this;
                  }

                  @Override
                  public Bucket.ObjectUpdater setContentType(String contentType) {
                    return this;
                  }

                  @Override
                  public MockObject commit() {
                    throw new UnsupportedOperationException("Injected Object Storage Failure");
                  }
                });
          }
          l.add(k);
          storedLocations.set(l);
          return Optional.empty();
        });

    soft.assertThatThrownBy(() -> commitMultiple(main, key1, key2, key3, key4))
        .isInstanceOf(ExecutionException.class)
        .cause()
        .isInstanceOf(S3Exception.class);
    soft.assertThat(storedLocations.get()).hasSize(after);
    soft.assertThat(failedLocations.get()).hasSize(4 - after);
    soft.assertThat(heapStorageBucket.objects()).isEmpty();
  }

  @Test
  public void noCommitOps() throws Exception {
    Reference main = api.getReference().refName("main").get();

    ParsedReference ref =
        parsedReference(main.getName(), main.getHash(), Reference.ReferenceType.BRANCH);
    CatalogCommit commit = CatalogCommit.builder().build();

    catalogService.commit(ref, commit).toCompletableFuture().get();

    Reference afterCommit = api.getReference().refName("main").get();
    soft.assertThat(afterCommit).isEqualTo(main);
  }

  /** Verifies that a single table create catalog-commit passes. */
  @Test
  public void twoTableCreates() throws Exception {
    Reference main = api.getReference().refName("main").get();
    ContentKey key1 = ContentKey.of("mytable1");
    ContentKey key2 = ContentKey.of("mytable2");

    ParsedReference committed = commitMultiple(main, key1, key2);

    Reference afterCommit = api.getReference().refName("main").get();
    soft.assertThat(afterCommit)
        .isNotEqualTo(main)
        .extracting(Reference::getName, Reference::getHash)
        .containsExactly(committed.name(), committed.hashWithRelativeSpec());
  }

  @Test
  public void singleTableCreate() throws Exception {
    Reference main = api.getReference().refName("main").get();
    ContentKey key = ContentKey.of("mytable");

    ParsedReference committed = commitSingle(main, key);

    Reference afterCommit = api.getReference().refName("main").get();
    soft.assertThat(afterCommit)
        .isNotEqualTo(main)
        .extracting(Reference::getName, Reference::getHash)
        .containsExactly(committed.name(), committed.hashWithRelativeSpec());

    SnapshotResponse snap =
        catalogService
            .retrieveSnapshot(
                forSnapshotHttpReq(committed, "ICEBERG", "2"), key, ICEBERG_TABLE, false)
            .toCompletableFuture()
            .get(5, MINUTES);

    soft.assertThat(snap)
        .extracting(
            SnapshotResponse::contentKey,
            SnapshotResponse::contentType,
            SnapshotResponse::effectiveReference)
        .containsExactly(key, "application/json", afterCommit);

    soft.assertThat(snap.content())
        .extracting(IcebergTable.class::cast)
        .extracting(IcebergTable::getMetadataLocation, STRING)
        .endsWith(".metadata.json");
    soft.assertThat(snap.entityObject()).containsInstanceOf(IcebergTableMetadata.class);

    IcebergTableMetadata icebergMetadataEntity =
        (IcebergTableMetadata) snap.entityObject().orElseThrow();

    IcebergTableMetadata icebergMetadata =
        NessieModelIceberg.nessieTableSnapshotToIceberg(
            (NessieTableSnapshot) snap.nessieSnapshot(),
            Optional.empty(),
            m -> m.putAll(icebergMetadataEntity.properties()));

    soft.assertThat(snap.entityObject()).contains(icebergMetadata);

    String expectedJson =
        IcebergJson.objectMapper()
            .writeValueAsString(
                IcebergTableMetadata.builder()
                    .from(icebergMetadataEntity)
                    .properties(Map.of())
                    .build());
    soft.assertThat(
            objectIO.readObject(
                StorageUri.of(((IcebergTable) snap.content()).getMetadataLocation())))
        .hasContent(expectedJson);
  }

  /**
   * Verify behavior of {@link CatalogService#retrieveSnapshot(SnapshotReqParams, ContentKey,
   * Content.Type, boolean)} against related Nessie {@link CheckType check types} for read and write
   * intents.
   */
  @Test
  public void retrieveSnapshotAccessChecks() throws Exception {
    Reference main = api.getReference().refName("main").get();
    ContentKey key = ContentKey.of("mytable");

    ParsedReference committed = commitSingle(main, key);

    AtomicReference<CheckType> failingCheckType = new AtomicReference<>();
    batchAccessCheckerFactory =
        x ->
            new AbstractBatchAccessChecker() {
              @Override
              public Map<Check, String> check() {
                return getChecks().stream()
                    .filter(c -> failingCheckType.get() == c.type())
                    .collect(Collectors.toMap(Function.identity(), Object::toString));
              }
            };

    List<CheckType> checks =
        asList(
            VIEW_REFERENCE,
            COMMIT_CHANGE_AGAINST_REFERENCE,
            READ_ENTITY_VALUE,
            UPDATE_ENTITY,
            null);
    for (CheckType checkType : checks) {
      boolean readFail = checkType == VIEW_REFERENCE || checkType == READ_ENTITY_VALUE;
      boolean writeFail =
          readFail || checkType == COMMIT_CHANGE_AGAINST_REFERENCE || checkType == UPDATE_ENTITY;
      failingCheckType.set(checkType);

      AbstractThrowableAssert<?, ? extends Throwable> read =
          soft.assertThatCode(
                  () ->
                      catalogService
                          .retrieveSnapshot(
                              forSnapshotHttpReq(committed, "ICEBERG", "2"),
                              key,
                              ICEBERG_TABLE,
                              false)
                          .toCompletableFuture()
                          .get(5, MINUTES))
              .describedAs("forRead with %s", checkType);
      if (readFail) {
        read.isInstanceOf(AccessCheckException.class);
      } else {
        read.doesNotThrowAnyException();
      }

      AbstractThrowableAssert<?, ? extends Throwable> write =
          soft.assertThatCode(
                  () ->
                      catalogService
                          .retrieveSnapshot(
                              forSnapshotHttpReq(committed, "ICEBERG", "2"),
                              key,
                              ICEBERG_TABLE,
                              true)
                          .toCompletableFuture()
                          .get(5, MINUTES))
              .describedAs("forWrite with %s", checkType);
      if (writeFail) {
        write.isInstanceOf(AccessCheckException.class);
      } else {
        write.doesNotThrowAnyException();
      }
    }
  }
}

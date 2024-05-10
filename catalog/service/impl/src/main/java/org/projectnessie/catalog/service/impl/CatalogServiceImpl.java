/*
 * Copyright (C) 2023 Dremio
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

import static java.lang.String.format;
import static java.util.Collections.emptyMap;
import static java.util.UUID.randomUUID;
import static java.util.concurrent.CompletableFuture.completedStage;
import static org.projectnessie.catalog.formats.iceberg.nessie.NessieModelIceberg.icebergMetadataToContent;
import static org.projectnessie.catalog.formats.iceberg.nessie.NessieModelIceberg.nessieTableSnapshotToIceberg;
import static org.projectnessie.catalog.formats.iceberg.nessie.NessieModelIceberg.nessieViewSnapshotToIceberg;
import static org.projectnessie.catalog.formats.iceberg.nessie.NessieModelIceberg.newIcebergTableSnapshot;
import static org.projectnessie.catalog.formats.iceberg.nessie.NessieModelIceberg.newIcebergViewSnapshot;
import static org.projectnessie.catalog.formats.iceberg.nessie.NessieModelIceberg.typeToEntityName;
import static org.projectnessie.catalog.service.api.NessieSnapshotResponse.nessieSnapshotResponse;
import static org.projectnessie.catalog.service.impl.Util.objIdToNessieId;
import static org.projectnessie.error.ReferenceConflicts.referenceConflicts;
import static org.projectnessie.model.Conflict.conflict;
import static org.projectnessie.model.Content.Type.ICEBERG_TABLE;
import static org.projectnessie.versioned.storage.common.persist.ObjIdHasher.objIdHasher;

import jakarta.enterprise.context.RequestScoped;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.projectnessie.api.v2.params.ParsedReference;
import org.projectnessie.catalog.files.api.ObjectIO;
import org.projectnessie.catalog.formats.iceberg.IcebergSpec;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergJson;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergTableMetadata;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergViewMetadata;
import org.projectnessie.catalog.formats.iceberg.nessie.IcebergTableMetadataUpdateState;
import org.projectnessie.catalog.formats.iceberg.nessie.IcebergViewMetadataUpdateState;
import org.projectnessie.catalog.formats.iceberg.rest.IcebergCatalogOperation;
import org.projectnessie.catalog.model.id.NessieId;
import org.projectnessie.catalog.model.ops.CatalogOperation;
import org.projectnessie.catalog.model.snapshot.NessieEntitySnapshot;
import org.projectnessie.catalog.model.snapshot.NessieTableSnapshot;
import org.projectnessie.catalog.model.snapshot.NessieViewSnapshot;
import org.projectnessie.catalog.service.api.CatalogCommit;
import org.projectnessie.catalog.service.api.CatalogEntityAlreadyExistsException;
import org.projectnessie.catalog.service.api.CatalogService;
import org.projectnessie.catalog.service.api.SnapshotFormat;
import org.projectnessie.catalog.service.api.SnapshotReqParams;
import org.projectnessie.catalog.service.api.SnapshotResponse;
import org.projectnessie.client.api.CommitMultipleOperationsBuilder;
import org.projectnessie.client.api.GetContentBuilder;
import org.projectnessie.client.api.NessieApiV2;
import org.projectnessie.error.BaseNessieClientServerException;
import org.projectnessie.error.ErrorCode;
import org.projectnessie.error.ImmutableNessieError;
import org.projectnessie.error.NessieContentNotFoundException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.error.NessieReferenceConflictException;
import org.projectnessie.model.Branch;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.CommitResponse;
import org.projectnessie.model.Conflict;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.ContentResponse;
import org.projectnessie.model.GetMultipleContentsResponse;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.IcebergView;
import org.projectnessie.model.Namespace;
import org.projectnessie.model.Operation;
import org.projectnessie.model.Reference;
import org.projectnessie.nessie.tasks.api.TasksService;
import org.projectnessie.versioned.storage.common.persist.ObjId;
import org.projectnessie.versioned.storage.common.persist.Persist;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RequestScoped
public class CatalogServiceImpl implements CatalogService {

  private static final Logger LOGGER = LoggerFactory.getLogger(CatalogServiceImpl.class);

  private final ObjectIO objectIO;
  private final NessieApiV2 nessieApi;
  private final Persist persist;
  private final TasksService tasksService;
  private final Executor executor;

  @SuppressWarnings("unused")
  public CatalogServiceImpl() {
    this(null, null, null, null, null);
  }

  @SuppressWarnings("CdiInjectionPointsInspection")
  @Inject
  public CatalogServiceImpl(
      ObjectIO objectIO,
      NessieApiV2 nessieApi,
      Persist persist,
      TasksService tasksService,
      @Named("import-jobs") Executor executor) {
    this.objectIO = objectIO;
    this.nessieApi = nessieApi;
    this.persist = persist;
    this.tasksService = tasksService;
    this.executor = executor;
  }

  @Override
  public Stream<Supplier<CompletionStage<SnapshotResponse>>> retrieveSnapshots(
      SnapshotReqParams reqParams,
      List<ContentKey> keys,
      CatalogUriResolver catalogUriResolver,
      Consumer<Reference> effectiveReferenceConsumer)
      throws NessieNotFoundException {
    ParsedReference reference = reqParams.ref();

    LOGGER.trace(
        "retrieveTableSnapshots ref-name:{} ref-hash:{} keys:{}",
        reference.name(),
        reference.hashWithRelativeSpec(),
        keys);

    GetMultipleContentsResponse contentResponse =
        nessieApi
            .getContent()
            .refName(reference.name())
            .hashOnRef(reference.hashWithRelativeSpec())
            .keys(keys)
            .getWithResponse();

    IcebergStuff icebergStuff = new IcebergStuff(objectIO, persist, tasksService, executor);

    Reference effectiveReference = contentResponse.getEffectiveReference();
    effectiveReferenceConsumer.accept(effectiveReference);
    return contentResponse.getContents().stream()
        .map(
            c -> {
              ObjId snapshotId;
              try {
                snapshotId = snapshotIdFromContent(c.getContent());
              } catch (Exception e) {
                e.printStackTrace();
                return null;
              }
              return (Supplier<CompletionStage<SnapshotResponse>>)
                  () -> {
                    ContentKey key = c.getKey();
                    LOGGER.trace(
                        "retrieveTableSnapshots - individual ref-name:{} ref-hash:{} key:{}",
                        reference.name(),
                        reference.hashWithRelativeSpec(),
                        key);
                    CompletionStage<NessieEntitySnapshot<?>> snapshotStage =
                        icebergStuff.retrieveIcebergSnapshot(
                            snapshotId, c.getContent(), reqParams.snapshotFormat());
                    return snapshotStage.thenApply(
                        snapshot ->
                            snapshotResponse(
                                key, reqParams, catalogUriResolver, snapshot, effectiveReference));
                  };
            })
        .filter(Objects::nonNull);
  }

  @Override
  public CompletionStage<SnapshotResponse> retrieveSnapshot(
      SnapshotReqParams reqParams,
      ContentKey key,
      CatalogUriResolver catalogUriResolver,
      Content.Type expectedType)
      throws NessieNotFoundException {

    ParsedReference reference = reqParams.ref();

    LOGGER.trace(
        "retrieveTableSnapshot ref-name:{} ref-hash:{} key:{}",
        reference.name(),
        reference.hashWithRelativeSpec(),
        key);

    ContentResponse contentResponse =
        nessieApi
            .getContent()
            .refName(reference.name())
            .hashOnRef(reference.hashWithRelativeSpec())
            .getSingle(key);
    Content content = contentResponse.getContent();
    if (!content.getType().equals(expectedType)) {
      throw new NessieContentNotFoundException(key, reference.name());
    }
    Reference effectiveReference = contentResponse.getEffectiveReference();

    ObjId snapshotId = snapshotIdFromContent(content);

    CompletionStage<NessieEntitySnapshot<?>> snapshotStage =
        new IcebergStuff(objectIO, persist, tasksService, executor)
            .retrieveIcebergSnapshot(snapshotId, content, reqParams.snapshotFormat());

    return snapshotStage.thenApply(
        snapshot ->
            snapshotResponse(key, reqParams, catalogUriResolver, snapshot, effectiveReference));
  }

  private SnapshotResponse snapshotResponse(
      ContentKey key,
      SnapshotReqParams reqParams,
      CatalogUriResolver catalogUriResolver,
      NessieEntitySnapshot<?> snapshot,
      Reference effectiveReference) {
    if (snapshot instanceof NessieTableSnapshot) {
      return snapshotTableResponse(
          key, reqParams, catalogUriResolver, (NessieTableSnapshot) snapshot, effectiveReference);
    }
    if (snapshot instanceof NessieViewSnapshot) {
      return snapshotViewResponse(
          key, reqParams, catalogUriResolver, (NessieViewSnapshot) snapshot, effectiveReference);
    }
    throw new IllegalArgumentException(
        "Unsupported snapshot type " + snapshot.getClass().getSimpleName());
  }

  private SnapshotResponse snapshotTableResponse(
      ContentKey key,
      SnapshotReqParams reqParams,
      CatalogUriResolver catalogUriResolver,
      NessieTableSnapshot snapshot,
      Reference effectiveReference) {
    Object result;
    String fileName;

    Consumer<Map<String, String>> tablePropertiesTweak =
        properties -> {
          properties.put("nessie.catalog.content-id", snapshot.entity().nessieContentId());
          properties.put("nessie.catalog.snapshot-id", snapshot.id().idAsString());
          properties.put("nessie.commit.id", effectiveReference.getHash());
          properties.put("nessie.commit.ref", effectiveReference.toPathString());
        };

    switch (reqParams.snapshotFormat()) {
      case NESSIE_SNAPSHOT:
        fileName =
            String.join("/", key.getElements())
                + '_'
                + snapshot.id().idAsString()
                + ".nessie-metadata.json";
        result = nessieSnapshotResponse(effectiveReference, snapshot);
        break;
      case ICEBERG_TABLE_METADATA:
        // Return the snapshot as an Iceberg table-metadata using either the spec-version
        // given in
        // the request or the one used when the table-metadata was written.
        // TODO Does requesting a table-metadata using another spec-version make any sense?
        // TODO Response should respect the JsonView / spec-version
        // TODO Add a check that the original table format was Iceberg (not Delta)
        result =
            nessieTableSnapshotToIceberg(
                snapshot, optionalIcebergSpec(reqParams.reqVersion()), tablePropertiesTweak);

        fileName = "00000-" + snapshot.id().idAsString() + ".metadata.json";
        break;
      default:
        throw new IllegalArgumentException("Unknown format " + reqParams.snapshotFormat());
    }

    return SnapshotResponse.forEntity(
        effectiveReference, result, fileName, "application/json", key, snapshot);
  }

  private SnapshotResponse snapshotViewResponse(
      ContentKey key,
      SnapshotReqParams reqParams,
      CatalogUriResolver catalogUriResolver,
      NessieViewSnapshot snapshot,
      Reference effectiveReference) {
    Object result;
    String fileName;

    Consumer<Map<String, String>> tablePropertiesTweak =
        properties -> {
          properties.put("nessie.catalog.content-id", snapshot.entity().nessieContentId());
          properties.put("nessie.catalog.snapshot-id", snapshot.id().idAsString());
          properties.put("nessie.commit.id", effectiveReference.getHash());
          properties.put("nessie.commit.ref", effectiveReference.toPathString());
        };

    switch (reqParams.snapshotFormat()) {
      case NESSIE_SNAPSHOT:
        fileName =
            String.join("/", key.getElements())
                + '_'
                + snapshot.id().idAsString()
                + ".nessie-metadata.json";
        result = nessieSnapshotResponse(effectiveReference, snapshot);
        break;
      case ICEBERG_TABLE_METADATA:
        // Return the snapshot as an Iceberg table-metadata using either the spec-version
        // given in
        // the request or the one used when the table-metadata was written.
        // TODO Does requesting a table-metadata using another spec-version make any sense?
        // TODO Response should respect the JsonView / spec-version
        // TODO Add a check that the original table format was Iceberg (not Delta)
        result =
            nessieViewSnapshotToIceberg(
                snapshot, optionalIcebergSpec(reqParams.reqVersion()), tablePropertiesTweak);

        fileName = "00000-" + snapshot.id().idAsString() + ".metadata.json";
        break;
      default:
        throw new IllegalArgumentException("Unknown format " + reqParams.snapshotFormat());
    }

    return SnapshotResponse.forEntity(
        effectiveReference, result, fileName, "application/json", key, snapshot);
  }

  @Override
  public CompletionStage<Stream<SnapshotResponse>> commit(
      ParsedReference reference,
      CatalogCommit commit,
      SnapshotReqParams reqParams,
      CatalogUriResolver catalogUriResolver)
      throws BaseNessieClientServerException {

    GetContentBuilder contentRequest =
        nessieApi
            .getContent()
            .refName(reference.name())
            .hashOnRef(reference.hashWithRelativeSpec());
    commit.getOperations().forEach(op -> contentRequest.key(op.getKey()));
    GetMultipleContentsResponse contentsResponse = contentRequest.getWithResponse();

    Branch target =
        Branch.of(
            reference.name(),
            reference.hashWithRelativeSpec() != null
                ? reference.hashWithRelativeSpec()
                : contentsResponse.getEffectiveReference().getHash());

    Map<ContentKey, Content> contents = contentsResponse.toContentsMap();

    IcebergStuff icebergStuff = new IcebergStuff(objectIO, persist, tasksService, executor);

    CommitMultipleOperationsBuilder nessieCommit =
        nessieApi.commitMultipleOperations().branch(target);

    MultiTableUpdate multiTableUpdate = new MultiTableUpdate(nessieCommit);

    LOGGER.trace(
        "Executing commit containing {} operations against '{}@{}'",
        commit.getOperations().size(),
        target.getName(),
        target.getHash());

    CompletionStage<MultiTableUpdate> commitBuilderStage = completedStage(null);
    StringBuilder message = new StringBuilder();
    if (commit.getOperations().size() > 1) {
      message.append("Catalog commit with ");
      message.append(commit.getOperations().size());
      message.append(" operations\n");
    }
    for (CatalogOperation op : commit.getOperations()) {
      Content content = contents.get(op.getKey());
      message
          .append(commit.getOperations().size() > 1 ? "\n* " : "")
          .append(contents.containsKey(op.getKey()) ? "Update" : "Create")
          .append(" ")
          .append(op.getType())
          .append(" ")
          .append(op.getKey());
      if (op.getType().equals(ICEBERG_TABLE)) {
        commitBuilderStage =
            applyIcebergTableCommitOperation(
                target, op, content, multiTableUpdate, commitBuilderStage);
      } else if (op.getType().equals(Content.Type.ICEBERG_VIEW)) {
        commitBuilderStage =
            applyIcebergViewCommitOperation(
                target, op, content, multiTableUpdate, commitBuilderStage);
      } else {
        throw new IllegalArgumentException("(Yet) unsupported entity type: " + op.getType());
      }
    }

    nessieCommit.commitMeta(CommitMeta.fromMessage(message.toString()));

    return commitBuilderStage.thenApply(
        updates -> {
          try {
            CommitResponse commitResponse = multiTableUpdate.nessieCommit.commitWithResponse();
            Map<ContentKey, String> addedContentsMap =
                commitResponse.getAddedContents() != null
                    ? commitResponse.toAddedContentsMap()
                    : emptyMap();
            for (SingleTableUpdate tableUpdate : multiTableUpdate.tableUpdates) {
              Content content = tableUpdate.content;
              if (content.getId() == null) {
                content = content.withId(addedContentsMap.get(tableUpdate.key));
              }
              // It is okay to ignore the returned `CompletionStage`, because the TasksService will
              // always trigger the operation, regardless whether the  `CompletionStage` is consumed
              // or not.
              NessieId snapshotId = objIdToNessieId(snapshotIdFromContent(content));
              icebergStuff.storeSnapshot(tableUpdate.snapshot.withId(snapshotId), content);
            }

            return multiTableUpdate.tableUpdates.stream()
                .map(
                    singleTableUpdate ->
                        snapshotResponse(
                            singleTableUpdate.key,
                            reqParams,
                            catalogUriResolver,
                            singleTableUpdate.snapshot,
                            commitResponse.getTargetBranch()));
          } catch (Exception e) {
            // TODO cleanup files that were written but are now obsolete/unreferenced
            throw new RuntimeException(e);
          }
        });
  }

  private CompletionStage<MultiTableUpdate> applyIcebergTableCommitOperation(
      Branch reference,
      CatalogOperation op,
      Content content,
      MultiTableUpdate multiTableUpdate,
      CompletionStage<MultiTableUpdate> commitBuilderStage)
      throws NessieContentNotFoundException, NessieReferenceConflictException {
    // TODO serialize the changes as well, so that we can retrieve those later for content-aware
    //  merges and automatic conflict resolution.

    IcebergCatalogOperation icebergOp = (IcebergCatalogOperation) op;

    if (icebergOp.hasAssertCreate()) {
      if (content != null) {
        throw new CatalogEntityAlreadyExistsException(
            true, op.getType(), op.getKey(), content.getType());
      }
    } else if (!op.getType().equals(content.getType())) {
      String msg =
          format(
              "Cannot update %s %s as a %s",
              typeToEntityName(content.getType()).toLowerCase(Locale.ROOT),
              op.getKey(),
              typeToEntityName(op.getType()).toLowerCase(Locale.ROOT));
      throw new NessieReferenceConflictException(
          referenceConflicts(conflict(Conflict.ConflictType.PAYLOAD_DIFFERS, op.getKey(), msg)),
          msg,
          null);
    }

    String contentId;
    CompletionStage<NessieTableSnapshot> snapshotStage;
    if (content == null) {
      contentId = null;
      snapshotStage = completedStage(newIcebergTableSnapshot(icebergOp.updates()));
    } else {
      contentId = content.getId();
      snapshotStage = loadExistingTableSnapshot(content);
    }

    CompletionStage<SingleTableUpdate> contentStage =
        snapshotStage
            .thenApply(
                nessieSnapshot -> {
                  if (LOGGER.isTraceEnabled()) {
                    LOGGER.trace(
                        "Applying {} metadata updates with {} requirements to '{}' against {}@{}",
                        icebergOp.updates().size(),
                        icebergOp.requirements().size(),
                        op.getKey(),
                        reference.getName(),
                        reference.getHash());
                  }
                  return new IcebergTableMetadataUpdateState(
                          nessieSnapshot, op.getKey(), content != null)
                      .checkRequirements(icebergOp.requirements())
                      .applyUpdates(icebergOp.updates())
                      .snapshot();
                  // TODO handle the case when nothing changed -> do not update
                  //  e.g. when adding a schema/spec/order that already exists
                })
            .thenApply(
                nessieSnapshot -> {
                  // TODO: support GZIP
                  // TODO: support TableProperties.WRITE_METADATA_LOCATION
                  String location =
                      String.format(
                          "%s/metadata/00000-%s.metadata.json",
                          nessieSnapshot.icebergLocation(), randomUUID());

                  URI uri = URI.create(location);
                  IcebergTableMetadata icebergMetadata = storeTableSnapshot(uri, nessieSnapshot);
                  Content updated = icebergMetadataToContent(location, icebergMetadata, contentId);

                  ObjId snapshotId;
                  try {
                    snapshotId = snapshotIdFromContent(updated);
                  } catch (Exception e) {
                    e.printStackTrace();
                    return null;
                  }
                  nessieSnapshot = nessieSnapshot.withId(objIdToNessieId(snapshotId));

                  SingleTableUpdate singleTableUpdate =
                      new SingleTableUpdate(nessieSnapshot, location, updated, icebergOp.getKey());
                  multiTableUpdate.addUpdate(op.getKey(), singleTableUpdate);
                  return singleTableUpdate;
                });

    // Form a chain of stages that complete sequentially and populate the commit builder.
    commitBuilderStage =
        contentStage.thenCombine(commitBuilderStage, (singleTableUpdate, nothing) -> null);
    return commitBuilderStage;
  }

  private CompletionStage<MultiTableUpdate> applyIcebergViewCommitOperation(
      Branch reference,
      CatalogOperation op,
      Content content,
      MultiTableUpdate multiTableUpdate,
      CompletionStage<MultiTableUpdate> commitBuilderStage)
      throws NessieContentNotFoundException {
    // TODO serialize the changes as well, so that we can retrieve those later for content-aware
    //  merges and automatic conflict resolution.

    IcebergCatalogOperation icebergOp = (IcebergCatalogOperation) op;

    String contentId;
    CompletionStage<NessieViewSnapshot> snapshotStage;
    if (content == null) {
      contentId = null;
      snapshotStage = completedStage(newIcebergViewSnapshot(icebergOp.updates()));
    } else {
      contentId = content.getId();
      snapshotStage = loadExistingViewSnapshot(content);
    }

    CompletionStage<SingleTableUpdate> contentStage =
        snapshotStage
            .thenApply(
                nessieSnapshot -> {
                  if (LOGGER.isTraceEnabled()) {
                    LOGGER.trace(
                        "Applying {} metadata updates with {} requirements to '{}' against {}@{}",
                        icebergOp.updates().size(),
                        icebergOp.requirements().size(),
                        op.getKey(),
                        reference.getName(),
                        reference.getHash());
                  }
                  return new IcebergViewMetadataUpdateState(
                          nessieSnapshot, op.getKey(), content != null)
                      .checkRequirements(icebergOp.requirements())
                      .applyUpdates(icebergOp.updates())
                      .snapshot();
                  // TODO handle the case when nothing changed -> do not update
                  //  e.g. when adding a schema/spec/order that already exists
                })
            .thenApply(
                nessieSnapshot -> {
                  // TODO: support GZIP ??
                  // TODO: support TableProperties.WRITE_METADATA_LOCATION
                  String location =
                      String.format(
                          "%s/metadata/00000-%s.metadata.json",
                          nessieSnapshot.icebergLocation(), randomUUID());

                  URI uri = URI.create(location);
                  IcebergViewMetadata icebergMetadata = storeViewSnapshot(uri, nessieSnapshot);
                  Content updated = icebergMetadataToContent(location, icebergMetadata, contentId);

                  ObjId snapshotId;
                  try {
                    snapshotId = snapshotIdFromContent(updated);
                  } catch (Exception e) {
                    e.printStackTrace();
                    return null;
                  }
                  nessieSnapshot = nessieSnapshot.withId(objIdToNessieId(snapshotId));

                  SingleTableUpdate singleTableUpdate =
                      new SingleTableUpdate(nessieSnapshot, location, updated, icebergOp.getKey());
                  multiTableUpdate.addUpdate(op.getKey(), singleTableUpdate);
                  return singleTableUpdate;
                });

    // Form a chain of stages that complete sequentially and populate the commit builder.
    commitBuilderStage =
        contentStage.thenCombine(commitBuilderStage, (singleTableUpdate, nothing) -> null);
    return commitBuilderStage;
  }

  static final class MultiTableUpdate {
    final CommitMultipleOperationsBuilder nessieCommit;
    final List<SingleTableUpdate> tableUpdates = new ArrayList<>();

    MultiTableUpdate(CommitMultipleOperationsBuilder nessieCommit) {
      this.nessieCommit = nessieCommit;
    }

    void addUpdate(ContentKey key, SingleTableUpdate singleTableUpdate) {
      synchronized (this) {
        tableUpdates.add(singleTableUpdate);
        nessieCommit.operation(Operation.Put.of(key, singleTableUpdate.content));
      }
    }
  }

  static final class SingleTableUpdate {
    final NessieEntitySnapshot<?> snapshot;
    final String location;
    final Content content;
    final ContentKey key;

    SingleTableUpdate(
        NessieEntitySnapshot<?> snapshot, String location, Content content, ContentKey key) {
      this.snapshot = snapshot;
      this.location = location;
      this.content = content;
      this.key = key;
    }
  }

  private CompletionStage<NessieTableSnapshot> loadExistingTableSnapshot(Content content)
      throws NessieContentNotFoundException {
    ObjId snapshotId = snapshotIdFromContent(content);
    return new IcebergStuff(objectIO, persist, tasksService, executor)
        .retrieveIcebergSnapshot(snapshotId, content, SnapshotFormat.NESSIE_SNAPSHOT);
  }

  private CompletionStage<NessieViewSnapshot> loadExistingViewSnapshot(Content content)
      throws NessieContentNotFoundException {
    ObjId snapshotId = snapshotIdFromContent(content);
    return new IcebergStuff(objectIO, persist, tasksService, executor)
        .retrieveIcebergSnapshot(snapshotId, content, SnapshotFormat.NESSIE_SNAPSHOT);
  }

  private IcebergTableMetadata storeTableSnapshot(URI location, NessieTableSnapshot snapshot) {
    IcebergTableMetadata tableMetadata =
        nessieTableSnapshotToIceberg(snapshot, Optional.empty(), p -> {});
    try (OutputStream out = objectIO.writeObject(location)) {
      IcebergJson.objectMapper().writeValue(out, tableMetadata);
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
    return tableMetadata;
  }

  private IcebergViewMetadata storeViewSnapshot(URI location, NessieViewSnapshot snapshot) {
    IcebergViewMetadata viewMetadata =
        nessieViewSnapshotToIceberg(snapshot, Optional.empty(), p -> {});
    try (OutputStream out = objectIO.writeObject(location)) {
      IcebergJson.objectMapper().writeValue(out, viewMetadata);
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
    return viewMetadata;
  }

  private static Optional<IcebergSpec> optionalIcebergSpec(OptionalInt specVersion) {
    return specVersion.isPresent()
        ? Optional.of(IcebergSpec.forVersion(specVersion.getAsInt()))
        : Optional.empty();
  }

  /** Compute the ID for the given Nessie {@link Content} object. */
  private ObjId snapshotIdFromContent(Content content) throws NessieContentNotFoundException {
    if (content instanceof IcebergTable) {
      IcebergTable icebergTable = (IcebergTable) content;
      return objIdHasher("ContentSnapshot")
          .hash(icebergTable.getMetadataLocation())
          .hash(icebergTable.getSnapshotId())
          .generate();
    }
    if (content instanceof IcebergView) {
      IcebergView icebergView = (IcebergView) content;
      return objIdHasher("ContentSnapshot")
          .hash(icebergView.getMetadataLocation())
          .hash(icebergView.getVersionId())
          .generate();
    }
    if (content instanceof Namespace) {
      throw new NessieContentNotFoundException(
          ImmutableNessieError.builder()
              .errorCode(ErrorCode.CONTENT_NOT_FOUND)
              .message("No snapshots for Namespace: " + content)
              .reason("Not a table")
              .status(404)
              .build());
    }
    throw new UnsupportedOperationException("IMPLEMENT ME FOR " + content);
  }
}

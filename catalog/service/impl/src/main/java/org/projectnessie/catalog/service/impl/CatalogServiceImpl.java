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

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedStage;
import static java.util.stream.Collectors.toList;
import static org.projectnessie.catalog.formats.iceberg.nessie.IcebergConstants.NESSIE_COMMIT_ID;
import static org.projectnessie.catalog.formats.iceberg.nessie.IcebergConstants.NESSIE_COMMIT_REF;
import static org.projectnessie.catalog.formats.iceberg.nessie.IcebergConstants.NESSIE_CONTENT_ID;
import static org.projectnessie.catalog.formats.iceberg.nessie.NessieModelIceberg.icebergBaseLocation;
import static org.projectnessie.catalog.formats.iceberg.nessie.NessieModelIceberg.icebergMetadataJsonLocation;
import static org.projectnessie.catalog.formats.iceberg.nessie.NessieModelIceberg.icebergMetadataToContent;
import static org.projectnessie.catalog.formats.iceberg.nessie.NessieModelIceberg.nessieTableSnapshotToIceberg;
import static org.projectnessie.catalog.formats.iceberg.nessie.NessieModelIceberg.nessieViewSnapshotToIceberg;
import static org.projectnessie.catalog.formats.iceberg.nessie.NessieModelIceberg.newIcebergTableSnapshot;
import static org.projectnessie.catalog.formats.iceberg.nessie.NessieModelIceberg.newIcebergViewSnapshot;
import static org.projectnessie.catalog.formats.iceberg.nessie.NessieModelIceberg.typeToEntityName;
import static org.projectnessie.catalog.formats.iceberg.rest.IcebergMetadataUpdate.SetProperties.setProperties;
import static org.projectnessie.catalog.formats.iceberg.rest.IcebergMetadataUpdate.SetTrustedLocation.setTrustedLocation;
import static org.projectnessie.catalog.service.api.NessieSnapshotResponse.nessieSnapshotResponse;
import static org.projectnessie.catalog.service.impl.Util.objIdToNessieId;
import static org.projectnessie.catalog.service.objtypes.EntitySnapshotObj.snapshotObjIdForContent;
import static org.projectnessie.error.ReferenceConflicts.referenceConflicts;
import static org.projectnessie.model.Conflict.conflict;
import static org.projectnessie.model.Content.Type.ICEBERG_TABLE;

import jakarta.annotation.Nullable;
import jakarta.enterprise.context.RequestScoped;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.projectnessie.api.v2.params.ParsedReference;
import org.projectnessie.catalog.files.api.BackendExceptionMapper;
import org.projectnessie.catalog.files.api.ObjectIO;
import org.projectnessie.catalog.formats.iceberg.IcebergSpec;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergJson;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergTableMetadata;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergViewMetadata;
import org.projectnessie.catalog.formats.iceberg.nessie.IcebergTableMetadataUpdateState;
import org.projectnessie.catalog.formats.iceberg.nessie.IcebergViewMetadataUpdateState;
import org.projectnessie.catalog.formats.iceberg.rest.IcebergCatalogOperation;
import org.projectnessie.catalog.formats.iceberg.rest.IcebergMetadataUpdate;
import org.projectnessie.catalog.formats.iceberg.rest.IcebergMetadataUpdate.AssignUUID;
import org.projectnessie.catalog.formats.iceberg.rest.IcebergMetadataUpdate.SetLocation;
import org.projectnessie.catalog.formats.iceberg.rest.IcebergMetadataUpdate.SetProperties;
import org.projectnessie.catalog.formats.iceberg.rest.IcebergUpdateRequirement.AssertCreate;
import org.projectnessie.catalog.model.id.NessieId;
import org.projectnessie.catalog.model.ops.CatalogOperation;
import org.projectnessie.catalog.model.snapshot.NessieEntitySnapshot;
import org.projectnessie.catalog.model.snapshot.NessieTableSnapshot;
import org.projectnessie.catalog.model.snapshot.NessieViewSnapshot;
import org.projectnessie.catalog.service.api.CatalogCommit;
import org.projectnessie.catalog.service.api.CatalogEntityAlreadyExistsException;
import org.projectnessie.catalog.service.api.CatalogService;
import org.projectnessie.catalog.service.api.SnapshotReqParams;
import org.projectnessie.catalog.service.api.SnapshotResponse;
import org.projectnessie.catalog.service.config.CatalogConfig;
import org.projectnessie.catalog.service.config.ServiceConfig;
import org.projectnessie.catalog.service.config.WarehouseConfig;
import org.projectnessie.catalog.service.impl.MultiTableUpdate.SingleTableUpdate;
import org.projectnessie.client.api.CommitMultipleOperationsBuilder;
import org.projectnessie.client.api.GetContentBuilder;
import org.projectnessie.client.api.NessieApiV2;
import org.projectnessie.error.BaseNessieClientServerException;
import org.projectnessie.error.NessieContentNotFoundException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.error.NessieReferenceConflictException;
import org.projectnessie.model.Branch;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.Conflict;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.ContentResponse;
import org.projectnessie.model.GetMultipleContentsResponse;
import org.projectnessie.model.Reference;
import org.projectnessie.nessie.tasks.api.TasksService;
import org.projectnessie.storage.uri.StorageUri;
import org.projectnessie.versioned.storage.common.persist.ObjId;
import org.projectnessie.versioned.storage.common.persist.Persist;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RequestScoped
public class CatalogServiceImpl implements CatalogService {

  private static final Logger LOGGER = LoggerFactory.getLogger(CatalogServiceImpl.class);

  @Inject ObjectIO objectIO;
  @Inject NessieApiV2 nessieApi;
  @Inject Persist persist;
  @Inject TasksService tasksService;
  @Inject BackendExceptionMapper backendExceptionMapper;
  @Inject CatalogConfig catalogConfig;
  @Inject ServiceConfig serviceConfig;

  @Inject
  @Named("import-jobs")
  Executor executor;

  private IcebergStuff icebergStuff() {
    return new IcebergStuff(
        objectIO,
        persist,
        tasksService,
        new EntitySnapshotTaskBehavior(
            backendExceptionMapper, serviceConfig.effectiveRetryAfterThrottled()),
        executor);
  }

  @Override
  public Stream<Supplier<CompletionStage<SnapshotResponse>>> retrieveSnapshots(
      SnapshotReqParams reqParams,
      List<ContentKey> keys,
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

    IcebergStuff icebergStuff = icebergStuff();

    Reference effectiveReference = contentResponse.getEffectiveReference();
    effectiveReferenceConsumer.accept(effectiveReference);
    return contentResponse.getContents().stream()
        .map(
            c -> {
              ObjId snapshotId;
              try {
                snapshotId = snapshotObjIdForContent(c.getContent());
              } catch (Exception e) {
                // This silently handles the case when `c` refers neither to an Iceberg table nor a
                // view.`
                LOGGER.debug(
                    "Failed to retrieve snapshot ID for {}: {}", c.getContent(), e.toString());
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
                        icebergStuff.retrieveIcebergSnapshot(snapshotId, c.getContent());
                    return snapshotStage.thenApply(
                        snapshot ->
                            snapshotResponse(
                                key, c.getContent(), reqParams, snapshot, effectiveReference));
                  };
            })
        .filter(Objects::nonNull);
  }

  @Override
  public CompletionStage<SnapshotResponse> retrieveSnapshot(
      SnapshotReqParams reqParams,
      ContentKey key,
      @Nullable Content.Type expectedType,
      boolean forWrite)
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
            .forWrite(forWrite)
            .getSingle(key);
    Content content = contentResponse.getContent();
    if (expectedType != null && !content.getType().equals(expectedType)) {
      throw new NessieContentNotFoundException(key, reference.name());
    }
    Reference effectiveReference = contentResponse.getEffectiveReference();

    ObjId snapshotId = snapshotObjIdForContent(content);

    CompletionStage<NessieEntitySnapshot<?>> snapshotStage =
        icebergStuff().retrieveIcebergSnapshot(snapshotId, content);

    return snapshotStage.thenApply(
        snapshot -> snapshotResponse(key, content, reqParams, snapshot, effectiveReference));
  }

  private SnapshotResponse snapshotResponse(
      ContentKey key,
      Content content,
      SnapshotReqParams reqParams,
      NessieEntitySnapshot<?> snapshot,
      Reference effectiveReference) {
    if (snapshot instanceof NessieTableSnapshot) {
      return snapshotTableResponse(
          key, content, reqParams, (NessieTableSnapshot) snapshot, effectiveReference);
    }
    if (snapshot instanceof NessieViewSnapshot) {
      return snapshotViewResponse(
          key, content, reqParams, (NessieViewSnapshot) snapshot, effectiveReference);
    }
    throw new IllegalArgumentException(
        "Unsupported snapshot type " + snapshot.getClass().getSimpleName());
  }

  private SnapshotResponse snapshotTableResponse(
      ContentKey key,
      Content content,
      SnapshotReqParams reqParams,
      NessieTableSnapshot snapshot,
      Reference effectiveReference) {
    Object result;
    String fileName;

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
                snapshot,
                optionalIcebergSpec(reqParams.reqVersion()),
                metadataPropertiesTweak(snapshot, effectiveReference));

        fileName = "00000-" + snapshot.id().idAsString() + ".metadata.json";
        break;
      default:
        throw new IllegalArgumentException("Unknown format " + reqParams.snapshotFormat());
    }

    return SnapshotResponse.forEntity(
        effectiveReference, result, fileName, "application/json", key, content, snapshot);
  }

  private Consumer<Map<String, String>> metadataPropertiesTweak(
      NessieEntitySnapshot<?> snapshot, Reference effectiveReference) {
    return properties -> {
      properties.put(NESSIE_CONTENT_ID, snapshot.entity().nessieContentId());
      properties.put(NESSIE_COMMIT_ID, effectiveReference.getHash());
      properties.put(NESSIE_COMMIT_REF, effectiveReference.getName());
    };
  }

  private SnapshotResponse snapshotViewResponse(
      ContentKey key,
      Content content,
      SnapshotReqParams reqParams,
      NessieViewSnapshot snapshot,
      Reference effectiveReference) {
    Object result;
    String fileName;

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
                snapshot,
                optionalIcebergSpec(reqParams.reqVersion()),
                metadataPropertiesTweak(snapshot, effectiveReference));

        fileName = "00000-" + snapshot.id().idAsString() + ".metadata.json";
        break;
      default:
        throw new IllegalArgumentException("Unknown format " + reqParams.snapshotFormat());
    }

    return SnapshotResponse.forEntity(
        effectiveReference, result, fileName, "application/json", key, content, snapshot);
  }

  CompletionStage<MultiTableUpdate> commit(ParsedReference reference, CatalogCommit commit)
      throws BaseNessieClientServerException {

    GetContentBuilder contentRequest =
        nessieApi
            .getContent()
            .refName(reference.name())
            .hashOnRef(reference.hashWithRelativeSpec())
            .forWrite(true);
    commit.getOperations().forEach(op -> contentRequest.key(op.getKey()));
    GetMultipleContentsResponse contentsResponse = contentRequest.getWithResponse();

    checkArgument(
        requireNonNull(contentsResponse.getEffectiveReference()) instanceof Branch,
        "Can only commit to a branch, but %s %s",
        contentsResponse.getEffectiveReference().getType(),
        reference.name());

    Branch target =
        Branch.of(
            reference.name(),
            reference.hashWithRelativeSpec() != null
                ? reference.hashWithRelativeSpec()
                : contentsResponse.getEffectiveReference().getHash());

    Map<ContentKey, Content> contents = contentsResponse.toContentsMap();

    IcebergStuff icebergStuff = icebergStuff();

    CommitMultipleOperationsBuilder nessieCommit =
        nessieApi.commitMultipleOperations().branch(target);

    MultiTableUpdate multiTableUpdate = new MultiTableUpdate(nessieCommit, target);

    LOGGER.trace(
        "Executing commit containing {} operations against '{}@{}'",
        commit.getOperations().size(),
        target.getName(),
        target.getHash());

    CompletionStage<MultiTableUpdate> commitBuilderStage = completedStage(multiTableUpdate);
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
        verifyIcebergOperation(op, reference, content);
        commitBuilderStage =
            applyIcebergTableCommitOperation(
                target, op, content, multiTableUpdate, commitBuilderStage);
      } else if (op.getType().equals(Content.Type.ICEBERG_VIEW)) {
        verifyIcebergOperation(op, reference, content);
        commitBuilderStage =
            applyIcebergViewCommitOperation(
                target, op, content, multiTableUpdate, commitBuilderStage);
      } else {
        throw new IllegalArgumentException("(Yet) unsupported entity type: " + op.getType());
      }
    }

    nessieCommit.commitMeta(CommitMeta.fromMessage(message.toString()));

    return commitBuilderStage
        // Perform the Nessie commit. At this point, all metadata files have been written.
        .thenApply(
            updates -> {
              try {
                return updates.commit();
              } catch (RuntimeException e) {
                throw e;
              } catch (Exception e) {
                throw new RuntimeException(e);
              }
            })
        // Add a failure handler, that cover the commitWithResponse() above but also all write
        // failure that can happen in the stages added by
        // applyIcebergTable/ViewCommitOperation().
        .whenComplete(
            (r, e) -> {
              if (e != null) {
                try {
                  objectIO.deleteObjects(
                      multiTableUpdate.storedLocations().stream()
                          .map(StorageUri::of)
                          .collect(toList()));
                } catch (Exception ex) {
                  e.addSuppressed(ex);
                }
              }
            })
        // Persist the Nessie catalog/snapshot objects. Those cannot be stored earlier in the
        // applyIcebergTable/ViewCommitOperation(), because those might not have a Nessie content-ID
        // at that point, because those have to be assigned during a Nessie commit for new contents.
        .thenCompose(
            updates -> {
              Map<ContentKey, String> addedContentsMap = updates.addedContentsMap();
              CompletionStage<NessieEntitySnapshot<?>> current =
                  CompletableFuture.completedStage(null);
              for (SingleTableUpdate tableUpdate : updates.tableUpdates()) {
                Content content = tableUpdate.content;
                if (content.getId() == null) {
                  // Need the content-ID especially to (eagerly) build the
                  // `NessieEntitySnapshot`.
                  content = content.withId(addedContentsMap.get(tableUpdate.key));
                }
                NessieId snapshotId = objIdToNessieId(snapshotObjIdForContent(content));

                // Although the `TasksService` triggers the operation regardless of whether the
                // `CompletionStage` returned by `storeSnapshot()` is consumed, we have to
                // "wait"
                // for those to complete so that we keep the "request scoped context" alive.
                CompletionStage<NessieEntitySnapshot<?>> stage =
                    icebergStuff.storeSnapshot(tableUpdate.snapshot.withId(snapshotId), content);
                if (current == null) {
                  current = stage;
                } else {
                  current = current.thenCombine(stage, (snap1, snap2) -> snap1);
                }
              }
              return current.thenApply(x -> updates);
            });
  }

  @Override
  public CompletionStage<Stream<SnapshotResponse>> commit(
      ParsedReference reference, CatalogCommit commit, SnapshotReqParams reqParams)
      throws BaseNessieClientServerException {
    return commit(reference, commit)
        // Finally, transform each MultiTableUpdate.SingleTableUpdate to a SnapshotResponse
        .thenApply(
            updates ->
                updates.tableUpdates().stream()
                    .map(
                        singleTableUpdate ->
                            snapshotResponse(
                                singleTableUpdate.key,
                                singleTableUpdate.content,
                                reqParams,
                                singleTableUpdate.snapshot,
                                updates.targetBranch())));
  }

  private static void verifyIcebergOperation(
      CatalogOperation op, ParsedReference reference, Content content)
      throws NessieContentNotFoundException, NessieReferenceConflictException {
    IcebergCatalogOperation icebergOp = (IcebergCatalogOperation) op;
    boolean hasAssertCreate = icebergOp.hasRequirement(AssertCreate.class);
    if (hasAssertCreate && content != null) {
      throw new CatalogEntityAlreadyExistsException(
          true, op.getType(), op.getKey(), content.getType());
    }
    if (!hasAssertCreate && content == null) {
      throw new NessieContentNotFoundException(op.getKey(), reference.name());
    }
    if (content != null) {
      if (!op.getType().equals(content.getType())) {
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
    }
  }

  private CompletionStage<MultiTableUpdate> applyIcebergTableCommitOperation(
      Branch reference,
      CatalogOperation op,
      Content content,
      MultiTableUpdate multiTableUpdate,
      CompletionStage<MultiTableUpdate> commitBuilderStage) {
    // TODO serialize the changes as well, so that we can retrieve those later for content-aware
    //  merges and automatic conflict resolution.

    IcebergCatalogOperation icebergOp = (IcebergCatalogOperation) op;

    String contentId;
    CompletionStage<NessieTableSnapshot> snapshotStage;
    if (content == null) {
      contentId = null;
      String icebergUuid = icebergOp.getSingleUpdateValue(AssignUUID.class, AssignUUID::uuid);
      snapshotStage = completedStage(newIcebergTableSnapshot(icebergUuid));
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
                      .applyUpdates(pruneUpdates(icebergOp, content != null))
                      .snapshot();
                  // TODO handle the case when nothing changed -> do not update
                  //  e.g. when adding a schema/spec/order that already exists
                })
            .thenApply(
                nessieSnapshot -> {
                  String metadataJsonLocation =
                      icebergMetadataJsonLocation(nessieSnapshot.icebergLocation());
                  IcebergTableMetadata icebergMetadata =
                      storeTableSnapshot(metadataJsonLocation, nessieSnapshot, multiTableUpdate);
                  Content updated =
                      icebergMetadataToContent(metadataJsonLocation, icebergMetadata, contentId);

                  ObjId snapshotId = snapshotObjIdForContent(updated);
                  nessieSnapshot = nessieSnapshot.withId(objIdToNessieId(snapshotId));

                  SingleTableUpdate singleTableUpdate =
                      new SingleTableUpdate(nessieSnapshot, updated, icebergOp.getKey());
                  multiTableUpdate.addUpdate(op.getKey(), singleTableUpdate);
                  return singleTableUpdate;
                });

    // Form a chain of stages that complete sequentially and populate the commit builder.
    commitBuilderStage =
        contentStage.thenCombine(
            commitBuilderStage, (singleTableUpdate, nothing) -> multiTableUpdate);
    return commitBuilderStage;
  }

  private CompletionStage<MultiTableUpdate> applyIcebergViewCommitOperation(
      Branch reference,
      CatalogOperation op,
      Content content,
      MultiTableUpdate multiTableUpdate,
      CompletionStage<MultiTableUpdate> commitBuilderStage) {
    // TODO serialize the changes as well, so that we can retrieve those later for content-aware
    //  merges and automatic conflict resolution.

    IcebergCatalogOperation icebergOp = (IcebergCatalogOperation) op;

    String contentId;
    CompletionStage<NessieViewSnapshot> snapshotStage;
    if (content == null) {
      contentId = null;
      String icebergUuid = icebergOp.getSingleUpdateValue(AssignUUID.class, AssignUUID::uuid);
      snapshotStage = completedStage(newIcebergViewSnapshot(icebergUuid));
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
                      .applyUpdates(pruneUpdates(icebergOp, content != null))
                      .snapshot();
                  // TODO handle the case when nothing changed -> do not update
                  //  e.g. when adding a schema/spec/order that already exists
                })
            .thenApply(
                nessieSnapshot -> {
                  String metadataJsonLocation =
                      icebergMetadataJsonLocation(nessieSnapshot.icebergLocation());
                  IcebergViewMetadata icebergMetadata =
                      storeViewSnapshot(metadataJsonLocation, nessieSnapshot, multiTableUpdate);
                  Content updated =
                      icebergMetadataToContent(metadataJsonLocation, icebergMetadata, contentId);
                  ObjId snapshotId = snapshotObjIdForContent(updated);
                  nessieSnapshot = nessieSnapshot.withId(objIdToNessieId(snapshotId));

                  SingleTableUpdate singleTableUpdate =
                      new SingleTableUpdate(nessieSnapshot, updated, icebergOp.getKey());
                  multiTableUpdate.addUpdate(op.getKey(), singleTableUpdate);
                  return singleTableUpdate;
                });

    // Form a chain of stages that complete sequentially and populate the commit builder.
    commitBuilderStage =
        contentStage.thenCombine(
            commitBuilderStage, (singleTableUpdate, nothing) -> multiTableUpdate);
    return commitBuilderStage;
  }

  private List<IcebergMetadataUpdate> pruneUpdates(IcebergCatalogOperation op, boolean update) {
    if (update) {
      return op.updates();
    }
    List<IcebergMetadataUpdate> prunedUpdates = new ArrayList<>(op.updates());
    String location = null;
    if (op.hasUpdate(SetProperties.class)) {
      Map<String, String> properties =
          op.getSingleUpdateValue(SetProperties.class, SetProperties::updates);
      if (properties.containsKey(IcebergTableMetadata.STAGED_PROPERTY)) {
        String stagedLocation = op.getSingleUpdateValue(SetLocation.class, SetLocation::location);
        // TODO verify integrity of staged location
        prunedUpdates.removeIf(u -> u instanceof SetProperties);
        properties = new HashMap<>(properties);
        properties.remove(IcebergTableMetadata.STAGED_PROPERTY);
        prunedUpdates.add(setProperties(properties));
        location = stagedLocation;
      }
    }
    if (location == null) {
      WarehouseConfig w = catalogConfig.getWarehouse(op.warehouse());
      location = icebergBaseLocation(w.location(), op.getKey());
    }
    prunedUpdates.add(setTrustedLocation(location));
    return prunedUpdates;
  }

  private CompletionStage<NessieTableSnapshot> loadExistingTableSnapshot(Content content) {
    ObjId snapshotId = snapshotObjIdForContent(content);
    return icebergStuff().retrieveIcebergSnapshot(snapshotId, content);
  }

  private CompletionStage<NessieViewSnapshot> loadExistingViewSnapshot(Content content) {
    ObjId snapshotId = snapshotObjIdForContent(content);
    return icebergStuff().retrieveIcebergSnapshot(snapshotId, content);
  }

  private IcebergTableMetadata storeTableSnapshot(
      String metadataJsonLocation,
      NessieTableSnapshot snapshot,
      MultiTableUpdate multiTableUpdate) {
    IcebergTableMetadata tableMetadata =
        nessieTableSnapshotToIceberg(snapshot, Optional.empty(), p -> {});
    return storeSnapshot(metadataJsonLocation, tableMetadata, multiTableUpdate);
  }

  private IcebergViewMetadata storeViewSnapshot(
      String metadataJsonLocation, NessieViewSnapshot snapshot, MultiTableUpdate multiTableUpdate) {
    IcebergViewMetadata viewMetadata =
        nessieViewSnapshotToIceberg(snapshot, Optional.empty(), p -> {});
    return storeSnapshot(metadataJsonLocation, viewMetadata, multiTableUpdate);
  }

  private <M> M storeSnapshot(
      String metadataJsonLocation, M metadata, MultiTableUpdate multiTableUpdate) {
    multiTableUpdate.addStoredLocation(metadataJsonLocation);
    try (OutputStream out = objectIO.writeObject(StorageUri.of(metadataJsonLocation))) {
      IcebergJson.objectMapper().writeValue(out, metadata);
    } catch (Exception ex) {
      throw new RuntimeException("Failed to write snapshot to: " + metadataJsonLocation, ex);
    }
    return metadata;
  }

  private static Optional<IcebergSpec> optionalIcebergSpec(OptionalInt specVersion) {
    return specVersion.isPresent()
        ? Optional.of(IcebergSpec.forVersion(specVersion.getAsInt()))
        : Optional.empty();
  }
}

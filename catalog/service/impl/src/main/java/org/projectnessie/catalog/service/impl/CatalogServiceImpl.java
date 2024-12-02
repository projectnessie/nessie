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
import static java.util.stream.Collectors.toCollection;
import static java.util.stream.Collectors.toList;
import static org.projectnessie.catalog.formats.iceberg.meta.IcebergTableMetadata.STAGED_PROPERTY;
import static org.projectnessie.catalog.formats.iceberg.nessie.IcebergConstants.NESSIE_COMMIT_ID;
import static org.projectnessie.catalog.formats.iceberg.nessie.IcebergConstants.NESSIE_COMMIT_REF;
import static org.projectnessie.catalog.formats.iceberg.nessie.IcebergConstants.NESSIE_CONTENT_ID;
import static org.projectnessie.catalog.formats.iceberg.nessie.NessieModelIceberg.icebergMetadataJsonLocation;
import static org.projectnessie.catalog.formats.iceberg.nessie.NessieModelIceberg.icebergMetadataToContent;
import static org.projectnessie.catalog.formats.iceberg.nessie.NessieModelIceberg.icebergNewEntityBaseLocation;
import static org.projectnessie.catalog.formats.iceberg.nessie.NessieModelIceberg.nessieTableSnapshotToIceberg;
import static org.projectnessie.catalog.formats.iceberg.nessie.NessieModelIceberg.nessieViewSnapshotToIceberg;
import static org.projectnessie.catalog.formats.iceberg.nessie.NessieModelIceberg.newIcebergTableSnapshot;
import static org.projectnessie.catalog.formats.iceberg.nessie.NessieModelIceberg.newIcebergViewSnapshot;
import static org.projectnessie.catalog.formats.iceberg.nessie.NessieModelIceberg.typeToEntityName;
import static org.projectnessie.catalog.formats.iceberg.rest.IcebergMetadataUpdate.SetLocation.setTrustedLocation;
import static org.projectnessie.catalog.formats.iceberg.rest.IcebergMetadataUpdate.SetProperties.setProperties;
import static org.projectnessie.catalog.service.api.NessieSnapshotResponse.nessieSnapshotResponse;
import static org.projectnessie.catalog.service.impl.Util.objIdToNessieId;
import static org.projectnessie.catalog.service.objtypes.EntitySnapshotObj.snapshotObjIdForContent;
import static org.projectnessie.error.ReferenceConflicts.referenceConflicts;
import static org.projectnessie.model.Conflict.conflict;
import static org.projectnessie.model.Content.Type.ICEBERG_TABLE;
import static org.projectnessie.model.Content.Type.NAMESPACE;
import static org.projectnessie.versioned.RequestMeta.API_READ;

import com.google.common.annotations.VisibleForTesting;
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
import java.util.function.Function;
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
import org.projectnessie.catalog.service.config.LakehouseConfig;
import org.projectnessie.catalog.service.config.ServiceConfig;
import org.projectnessie.catalog.service.config.WarehouseConfig;
import org.projectnessie.catalog.service.impl.MultiTableUpdate.SingleTableUpdate;
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
import org.projectnessie.model.Namespace;
import org.projectnessie.model.Reference;
import org.projectnessie.nessie.tasks.api.TasksService;
import org.projectnessie.services.authz.AccessContext;
import org.projectnessie.services.authz.ApiContext;
import org.projectnessie.services.authz.Authorizer;
import org.projectnessie.services.config.ServerConfig;
import org.projectnessie.services.impl.ContentApiImpl;
import org.projectnessie.services.impl.TreeApiImpl;
import org.projectnessie.services.spi.ContentService;
import org.projectnessie.services.spi.TreeService;
import org.projectnessie.storage.uri.StorageUri;
import org.projectnessie.versioned.RequestMeta;
import org.projectnessie.versioned.RequestMeta.RequestMetaBuilder;
import org.projectnessie.versioned.VersionStore;
import org.projectnessie.versioned.storage.common.persist.ObjId;
import org.projectnessie.versioned.storage.common.persist.Persist;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("CdiInjectionPointsInspection")
@RequestScoped
public class CatalogServiceImpl implements CatalogService {

  private static final Logger LOGGER = LoggerFactory.getLogger(CatalogServiceImpl.class);

  @Inject ObjectIO objectIO;
  @Inject ServerConfig serverConfig;
  @Inject LakehouseConfig lakehouseConfig;
  @Inject VersionStore versionStore;
  @Inject Authorizer authorizer;
  @Inject AccessContext accessContext;
  @Inject Persist persist;
  @Inject TasksService tasksService;
  @Inject BackendExceptionMapper backendExceptionMapper;
  @Inject ServiceConfig serviceConfig;

  @Inject
  @Named("import-jobs")
  Executor executor;

  TreeService treeService(ApiContext apiContext) {
    return new TreeApiImpl(serverConfig, versionStore, authorizer, accessContext, apiContext);
  }

  ContentService contentService(ApiContext apiContext) {
    return new ContentApiImpl(serverConfig, versionStore, authorizer, accessContext, apiContext);
  }

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
  public Optional<String> validateStorageLocation(String location) {
    StorageUri uri = StorageUri.of(location);
    return objectIO.canResolve(uri);
  }

  @Override
  public StorageUri locationForEntity(
      WarehouseConfig warehouse,
      ContentKey contentKey,
      Content.Type contentType,
      ApiContext apiContext,
      String refName,
      String hash) {
    List<String> keyElements = contentKey.getElements();
    int keyElementCount = keyElements.size();

    List<ContentKey> keysInOrder = new ArrayList<>(keyElementCount);
    for (int i = 0; i < keyElementCount; i++) {
      ContentKey key = ContentKey.of(keyElements.subList(0, i + 1));
      keysInOrder.add(key);
    }

    GetMultipleContentsResponse namespaces;
    try {
      namespaces =
          contentService(apiContext)
              .getMultipleContents(refName, hash, keysInOrder, false, API_READ);
    } catch (NessieNotFoundException e) {
      throw new RuntimeException(e);
    }
    Map<ContentKey, Content> contentsMap = namespaces.toContentsMap();

    return locationForEntity(warehouse, contentKey, keysInOrder, contentsMap);
  }

  @Override
  public StorageUri locationForEntity(
      WarehouseConfig warehouse,
      ContentKey contentKey,
      List<ContentKey> keysInOrder,
      Map<ContentKey, Content> contentsMap) {
    List<String> keyElements = contentKey.getElements();
    int keyElementCount = keyElements.size();
    StorageUri location = null;
    List<String> remainingElements = List.of();

    // Find the nearest namespace with a 'location' property and start from there
    for (int n = keysInOrder.size() - 1; n >= 0; n--) {
      Content parent = contentsMap.get(keysInOrder.get(n));
      if (parent != null && parent.getType().equals(NAMESPACE)) {
        Namespace parentNamespace = (Namespace) parent;
        String parentLocation = parentNamespace.getProperties().get("location");
        if (parentLocation != null) {
          location = StorageUri.of(parentLocation).withTrailingSeparator();
          remainingElements = keyElements.subList(n + 1, keyElementCount);
        }
      }
    }

    // No parent namespace has a 'location' property, start from the warehouse
    if (location == null) {
      location = StorageUri.of(warehouse.location()).withTrailingSeparator();
      remainingElements = keyElements;
    }

    for (String element : remainingElements) {
      location = location.withTrailingSeparator().resolve(element);
    }

    return location;
  }

  @Override
  public Stream<Supplier<CompletionStage<SnapshotResponse>>> retrieveSnapshots(
      SnapshotReqParams reqParams,
      List<ContentKey> keys,
      Consumer<Reference> effectiveReferenceConsumer,
      RequestMeta requestMeta,
      ApiContext apiContext)
      throws NessieNotFoundException {
    ParsedReference reference = reqParams.ref();

    LOGGER.trace(
        "retrieveTableSnapshots ref-name:{} ref-hash:{} keys:{}",
        reference.name(),
        reference.hashWithRelativeSpec(),
        keys);

    GetMultipleContentsResponse contentResponse =
        contentService(apiContext)
            .getMultipleContents(
                reference.name(), reference.hashWithRelativeSpec(), keys, false, requestMeta);

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
      RequestMeta requestMeta,
      ApiContext apiContext)
      throws NessieNotFoundException {

    ParsedReference reference = reqParams.ref();

    LOGGER.trace(
        "retrieveTableSnapshot ref-name:{} ref-hash:{} key:{}",
        reference.name(),
        reference.hashWithRelativeSpec(),
        key);

    ContentResponse contentResponse =
        contentService(apiContext)
            .getContent(
                key, reference.name(), reference.hashWithRelativeSpec(), false, requestMeta);
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

  CompletionStage<MultiTableUpdate> commit(
      ParsedReference reference,
      CatalogCommit commit,
      Function<String, CommitMeta> commitMetaBuilder,
      String apiRequest,
      ApiContext apiContext)
      throws BaseNessieClientServerException {

    RequestMetaBuilder requestMeta = RequestMeta.apiWrite();
    List<ContentKey> allKeys =
        commit.getOperations().stream().map(CatalogOperation::getKey).collect(toList());
    for (ContentKey key : allKeys) {
      requestMeta.addKeyAction(key, apiRequest);
    }

    GetMultipleContentsResponse contentsResponse =
        contentService(apiContext)
            .getMultipleContents(
                reference.name(),
                reference.hashWithRelativeSpec(),
                allKeys,
                false,
                requestMeta.build());

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

    MultiTableUpdate multiTableUpdate =
        new MultiTableUpdate(treeService(apiContext), target, requestMeta);

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
                target, op, content, multiTableUpdate, commitBuilderStage, apiContext);
      } else if (op.getType().equals(Content.Type.ICEBERG_VIEW)) {
        verifyIcebergOperation(op, reference, content);
        commitBuilderStage =
            applyIcebergViewCommitOperation(
                target, op, content, multiTableUpdate, commitBuilderStage, apiContext);
      } else {
        throw new IllegalArgumentException("(Yet) unsupported entity type: " + op.getType());
      }
    }

    multiTableUpdate.operations().commitMeta(commitMetaBuilder.apply(message.toString()));

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
                current = current.thenCombine(stage, (snap1, snap2) -> snap1);
              }
              return current.thenApply(x -> updates);
            });
  }

  @Override
  public CompletionStage<Stream<SnapshotResponse>> commit(
      ParsedReference reference,
      CatalogCommit commit,
      SnapshotReqParams reqParams,
      Function<String, CommitMeta> commitMetaBuilder,
      String apiRequest,
      ApiContext apiContext)
      throws BaseNessieClientServerException {
    return commit(reference, commit, commitMetaBuilder, apiRequest, apiContext)
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
      CompletionStage<MultiTableUpdate> commitBuilderStage,
      ApiContext apiContext) {
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
                      .applyUpdates(
                          pruneUpdates(reference, icebergOp, content != null, apiContext));
                  // TODO handle the case when nothing changed -> do not update
                  //  e.g. when adding a schema/spec/order that already exists
                })
            .thenApply(
                updateState -> {
                  NessieTableSnapshot nessieSnapshot = updateState.snapshot();
                  String metadataJsonLocation =
                      icebergMetadataJsonLocation(nessieSnapshot.icebergLocation());
                  IcebergTableMetadata icebergMetadata =
                      storeTableSnapshot(metadataJsonLocation, nessieSnapshot, multiTableUpdate);
                  Content updated =
                      icebergMetadataToContent(metadataJsonLocation, icebergMetadata, contentId);

                  ObjId snapshotId = snapshotObjIdForContent(updated);
                  nessieSnapshot = nessieSnapshot.withId(objIdToNessieId(snapshotId));

                  SingleTableUpdate singleTableUpdate =
                      new SingleTableUpdate(
                          nessieSnapshot, updated, icebergOp.getKey(), updateState.catalogOps());
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
      CompletionStage<MultiTableUpdate> commitBuilderStage,
      ApiContext apiContext) {
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
                      .applyUpdates(
                          pruneUpdates(reference, icebergOp, content != null, apiContext));
                  // TODO handle the case when nothing changed -> do not update
                  //  e.g. when adding a schema/spec/order that already exists
                })
            .thenApply(
                updateState -> {
                  NessieViewSnapshot nessieSnapshot = updateState.snapshot();
                  String metadataJsonLocation =
                      icebergMetadataJsonLocation(nessieSnapshot.icebergLocation());
                  IcebergViewMetadata icebergMetadata =
                      storeViewSnapshot(metadataJsonLocation, nessieSnapshot, multiTableUpdate);
                  Content updated =
                      icebergMetadataToContent(metadataJsonLocation, icebergMetadata, contentId);
                  ObjId snapshotId = snapshotObjIdForContent(updated);
                  nessieSnapshot = nessieSnapshot.withId(objIdToNessieId(snapshotId));

                  SingleTableUpdate singleTableUpdate =
                      new SingleTableUpdate(
                          nessieSnapshot, updated, icebergOp.getKey(), updateState.catalogOps());
                  multiTableUpdate.addUpdate(op.getKey(), singleTableUpdate);
                  return singleTableUpdate;
                });

    // Form a chain of stages that complete sequentially and populate the commit builder.
    commitBuilderStage =
        contentStage.thenCombine(
            commitBuilderStage, (singleTableUpdate, nothing) -> multiTableUpdate);
    return commitBuilderStage;
  }

  private List<IcebergMetadataUpdate> pruneUpdates(
      Branch reference, IcebergCatalogOperation op, boolean update, ApiContext apiContext) {
    if (update) {
      return op.updates();
    }

    var prunedUpdates = pruneCreateUpdates(op.updates());

    var location = setLocationForCreate(reference, op, apiContext);
    prunedUpdates.add(setTrustedLocation(location));

    return prunedUpdates;
  }

  @VisibleForTesting
  String setLocationForCreate(Branch reference, IcebergCatalogOperation op, ApiContext apiContext) {
    return op.updates().stream()
        .filter(SetLocation.class::isInstance)
        .map(SetLocation.class::cast)
        .map(SetLocation::location)
        // Consider the _last_ SetLocation update, if there are multiple (not meaningful)
        .reduce((a, b) -> b)
        .map(
            l -> {
              // Validate externally provided storage locations
              validateStorageLocation(l)
                  .ifPresent(
                      msg -> {
                        throw new IllegalArgumentException(
                            format(
                                "Location for %s '%s' cannot be associated with any configured object storage location: %s",
                                op.getType().name(), op.getKey(), msg));
                      });
              return l;
            })
        .orElseGet(
            () -> {
              // Apply the computed & trusted storage location
              WarehouseConfig w = lakehouseConfig.catalog().getWarehouse(op.warehouse());
              return icebergNewEntityBaseLocation(
                  locationForEntity(
                          w,
                          op.getKey(),
                          op.getType(),
                          apiContext,
                          reference.getName(),
                          reference.getHash())
                      .toString());
            });
  }

  @VisibleForTesting
  static List<IcebergMetadataUpdate> pruneCreateUpdates(List<IcebergMetadataUpdate> updates) {
    // Iterate over all updates and tweak the 'SetProperties' updates to remove the
    // IcebergTableMetadata.STAGED_PROPERTY property, if present.
    return updates.stream()
        .map(
            up -> {
              if (up instanceof SetProperties) {
                var properties = ((SetProperties) up).updates();
                if (properties.containsKey(STAGED_PROPERTY)) {
                  properties = new HashMap<>(properties);
                  properties.remove(STAGED_PROPERTY);
                  if (properties.isEmpty()) {
                    return null;
                  }
                  return setProperties(properties);
                }
              }
              return up;
            })
        .filter(Objects::nonNull)
        .collect(toCollection(ArrayList::new));
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

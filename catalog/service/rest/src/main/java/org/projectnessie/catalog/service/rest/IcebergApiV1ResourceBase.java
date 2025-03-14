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
package org.projectnessie.catalog.service.rest;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.projectnessie.api.v2.params.ParsedReference.parsedReference;
import static org.projectnessie.api.v2.params.ReferenceResolver.resolveReferencePathElement;
import static org.projectnessie.catalog.formats.iceberg.nessie.IcebergConstants.DERIVED_PROPERTIES;
import static org.projectnessie.catalog.formats.iceberg.nessie.NessieModelIceberg.icebergNewEntityBaseLocation;
import static org.projectnessie.catalog.formats.iceberg.nessie.NessieModelIceberg.typeToEntityName;
import static org.projectnessie.catalog.formats.iceberg.rest.IcebergMetadataUpdate.SetLocation.setTrustedLocation;
import static org.projectnessie.catalog.service.rest.DecodedPrefix.decodedPrefix;
import static org.projectnessie.catalog.service.rest.NamespaceRef.namespaceRef;
import static org.projectnessie.catalog.service.rest.TableRef.tableRef;
import static org.projectnessie.catalog.service.rest.TimestampParser.timestampToNessie;
import static org.projectnessie.model.Content.Type.ICEBERG_TABLE;
import static org.projectnessie.model.Namespace.Empty.EMPTY_NAMESPACE;
import static org.projectnessie.model.Reference.ReferenceType.BRANCH;
import static org.projectnessie.services.authz.ApiContext.apiContext;
import static org.projectnessie.services.impl.RefUtil.toReference;
import static org.projectnessie.versioned.RequestMeta.API_READ;
import static org.projectnessie.versioned.RequestMeta.API_WRITE;

import com.google.common.base.Splitter;
import io.smallrye.mutiny.Uni;
import java.io.IOException;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Stream;
import org.projectnessie.api.v2.params.ParsedReference;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergTableIdentifier;
import org.projectnessie.catalog.formats.iceberg.nessie.CatalogOps;
import org.projectnessie.catalog.formats.iceberg.rest.IcebergCatalogOperation;
import org.projectnessie.catalog.formats.iceberg.rest.IcebergMetadataUpdate;
import org.projectnessie.catalog.formats.iceberg.rest.IcebergRenameTableRequest;
import org.projectnessie.catalog.formats.iceberg.rest.IcebergUpdateEntityRequest;
import org.projectnessie.catalog.service.api.CatalogCommit;
import org.projectnessie.catalog.service.api.CatalogEntityAlreadyExistsException;
import org.projectnessie.catalog.service.api.SnapshotReqParams;
import org.projectnessie.catalog.service.api.SnapshotResponse;
import org.projectnessie.catalog.service.config.LakehouseConfig;
import org.projectnessie.catalog.service.config.WarehouseConfig;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieContentNotFoundException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Branch;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.ContentResponse;
import org.projectnessie.model.EntriesResponse;
import org.projectnessie.model.GetMultipleContentsResponse;
import org.projectnessie.model.ImmutableEntriesResponse;
import org.projectnessie.model.ImmutableOperations;
import org.projectnessie.model.Namespace;
import org.projectnessie.model.Operation;
import org.projectnessie.model.Operations;
import org.projectnessie.model.Reference;
import org.projectnessie.model.TableReference;
import org.projectnessie.services.authz.AccessContext;
import org.projectnessie.services.authz.ApiContext;
import org.projectnessie.services.authz.Authorizer;
import org.projectnessie.services.config.ServerConfig;
import org.projectnessie.services.impl.ContentApiImpl;
import org.projectnessie.services.impl.TreeApiImpl;
import org.projectnessie.services.spi.ContentService;
import org.projectnessie.services.spi.PagedCountingResponseHandler;
import org.projectnessie.services.spi.TreeService;
import org.projectnessie.versioned.RequestMeta;
import org.projectnessie.versioned.RequestMeta.RequestMetaBuilder;
import org.projectnessie.versioned.VersionStore;

abstract class IcebergApiV1ResourceBase extends AbstractCatalogResource {

  final TreeService treeService;
  final ContentService contentService;
  final ServerConfig serverConfig;
  final LakehouseConfig lakehouseConfig;

  static final ApiContext ICEBERG_V1 = apiContext("Iceberg", 1);

  protected IcebergApiV1ResourceBase(
      ServerConfig serverConfig,
      LakehouseConfig lakehouseConfig,
      VersionStore store,
      Authorizer authorizer,
      AccessContext accessContext) {
    this.serverConfig = serverConfig;
    this.lakehouseConfig = lakehouseConfig;
    this.treeService = new TreeApiImpl(serverConfig, store, authorizer, accessContext, ICEBERG_V1);
    this.contentService =
        new ContentApiImpl(serverConfig, store, authorizer, accessContext, ICEBERG_V1);
  }

  protected Stream<EntriesResponse.Entry> listContent(
      NamespaceRef namespaceRef,
      String contentType,
      String pageToken,
      Integer pageSize,
      boolean withContent,
      Consumer<String> responsePagingToken)
      throws NessieNotFoundException {

    // Just in case...
    if ("null".equals(pageToken)) {
      pageToken = null;
    }

    if (pageSize == null) {
      pageToken = null;
    } else {
      // Hard-coded limit of 500 entries to return in one page
      pageSize = Math.max(1, Math.min(pageSize, 500));
      if (pageToken != null && pageToken.isEmpty()) {
        pageToken = null;
      }
    }

    Namespace namespace = namespaceRef.namespace();
    String celFilter =
        format(
            "entry.contentType == '%s' && size(entry.keyElements) == %d",
            contentType,
            namespace != null && !namespace.isEmpty() ? namespace.getElementCount() + 1 : 1);

    ImmutableEntriesResponse.Builder builder = EntriesResponse.builder();
    EntriesResponse entriesResponse =
        treeService.getEntries(
            namespaceRef.referenceName(),
            namespaceRef.hashWithRelativeSpec(),
            null,
            celFilter,
            pageToken,
            withContent,
            new PagedCountingResponseHandler<>(pageSize) {
              @Override
              public EntriesResponse build() {
                return builder.build();
              }

              @Override
              protected boolean doAddEntry(EntriesResponse.Entry entry) {
                builder.addEntries(entry);
                return true;
              }

              @Override
              public void hasMore(String pagingToken) {
                builder.isHasMore(true).token(pagingToken);
              }
            },
            h -> builder.effectiveReference(toReference(h)),
            null,
            null,
            namespace != null ? namespace.toContentKey() : null,
            List.of());

    String token = entriesResponse.getToken();
    if (token != null) {
      responsePagingToken.accept(token);
    }

    return entriesResponse.getEntries().stream();
  }

  protected void renameContent(
      String prefix, IcebergRenameTableRequest renameTableRequest, Content.Type expectedContentType)
      throws NessieNotFoundException, NessieConflictException {
    TableRef fromTableRef = decodeTableRef(prefix, renameTableRequest.source());
    TableRef toTableRef = decodeTableRef(prefix, renameTableRequest.destination());

    ParsedReference ref = requireNonNull(fromTableRef.reference());
    GetMultipleContentsResponse contents =
        contentService.getMultipleContents(
            ref.name(),
            ref.hashWithRelativeSpec(),
            List.of(toTableRef.contentKey(), fromTableRef.contentKey()),
            false,
            API_READ);
    Map<ContentKey, Content> contentsMap = contents.toContentsMap();
    Content existingFrom = contentsMap.get(fromTableRef.contentKey());
    if (existingFrom == null || !expectedContentType.equals(existingFrom.getType())) {
      throw new NessieContentNotFoundException(
          fromTableRef.contentKey(), renameTableRequest.source().name());
    }

    Reference effectiveRef = contents.getEffectiveReference();

    Content existingTo = contentsMap.get(toTableRef.contentKey());
    if (existingTo != null) {
      // TODO throw ViewAlreadyExistsError ?
      // TODO throw TableAlreadyExistsError ?
      throw new CatalogEntityAlreadyExistsException(
          expectedContentType,
          fromTableRef.contentKey(),
          existingTo.getType(),
          toTableRef.contentKey());
    }

    String entityType = typeToEntityName(expectedContentType).toLowerCase(Locale.ROOT);
    checkArgument(
        effectiveRef instanceof Branch,
        format("Must only rename a %s on a branch, but target is %s", entityType, effectiveRef));

    Operations ops =
        ImmutableOperations.builder()
            .addOperations(
                Operation.Delete.of(fromTableRef.contentKey()),
                Operation.Put.of(toTableRef.contentKey(), existingFrom))
            .commitMeta(
                updateCommitMeta(
                    format(
                        "rename %s %s to %s",
                        entityType, fromTableRef.contentKey(), toTableRef.contentKey())))
            .build();

    RequestMetaBuilder requestMeta =
        RequestMeta.apiWrite()
            .addKeyAction(fromTableRef.contentKey(), CatalogOps.CATALOG_RENAME_ENTITY_FROM.name())
            .addKeyAction(toTableRef.contentKey(), CatalogOps.CATALOG_RENAME_ENTITY_TO.name());

    treeService.commitMultipleOperations(
        effectiveRef.getName(), effectiveRef.getHash(), ops, requestMeta.build());
  }

  protected NamespaceRef decodeNamespaceRef(String prefix, String encodedNs) {
    DecodedPrefix decoded = decodePrefix(prefix);
    return decodeNamespaceRef(decoded, encodedNs);
  }

  protected NamespaceRef decodeNamespaceRef(DecodedPrefix decoded, String encodedNs) {
    ParsedReference ref = decoded.parsedReference();
    Namespace ns = decodeNamespace(encodedNs);
    return namespaceRef(ns, ref.name(), ref.hashWithRelativeSpec(), decoded.warehouse());
  }

  public TableRef decodeTableRef(String prefix, String encodedNs, String table) {
    Namespace ns = decodeNamespace(encodedNs);
    TableReference tableReference = TableReference.parse(table);

    return fixupTableRef(prefix, tableReference, ns);
  }

  public TableRef decodeTableRef(String prefix, IcebergTableIdentifier table) {
    TableReference tableReference = TableReference.parse(table.name());
    Namespace ns = Namespace.of(table.namespace().levels());

    return fixupTableRef(prefix, tableReference, ns);
  }

  private TableRef fixupTableRef(String prefix, TableReference tableReference, Namespace ns) {
    DecodedPrefix decoded = decodePrefix(prefix);
    ParsedReference ref = decoded.parsedReference();
    ContentKey contentKey = ContentKey.of(ns, tableReference.getName());

    String tableRef = tableReference.getReference();
    String refName = tableRef != null ? tableRef : ref.name();
    String refHash = ref.hashWithRelativeSpec();
    if (tableReference.hasHash()) {
      refHash = tableReference.getHash();
    } else if (tableReference.hasTimestamp()) {
      refHash = timestampToNessie(tableReference.getTimestamp());
    }

    return tableRef(contentKey, parsedReference(refName, refHash, null), decoded.warehouse());
  }

  public static Namespace decodeNamespace(String encodedNs) {
    if (encodedNs == null) {
      return EMPTY_NAMESPACE;
    }

    return Namespace.of(NAMESPACE_ESCAPED_SPLITTER.splitToList(encodedNs).toArray(new String[0]));
  }

  public static final char SEPARATOR = '\u001f';
  private static final String DEFAULT_REF_IN_PATH = "-";
  private static final Splitter NAMESPACE_ESCAPED_SPLITTER = Splitter.on(SEPARATOR);

  protected DecodedPrefix decodePrefix(String prefix) {
    String warehouse = null;
    ParsedReference parsedReference = null;
    if (prefix != null) {
      prefix = prefix.replace(SEPARATOR, '/');

      int indexAt = prefix.indexOf('|');
      if (indexAt != -1) {
        if (indexAt != prefix.length() - 1) {
          warehouse = prefix.substring(indexAt + 1);
        }
        prefix = prefix.substring(0, indexAt);
      }

      if (!prefix.isEmpty() && !DEFAULT_REF_IN_PATH.equals(prefix)) {
        parsedReference = resolveReferencePathElement(prefix, null, serverConfig::getDefaultBranch);
      }
    }

    if (parsedReference == null) {
      parsedReference =
          ParsedReference.parsedReference(serverConfig.getDefaultBranch(), null, BRANCH);
    }

    String resolvedWarehouse = lakehouseConfig.catalog().resolveWarehouseName(warehouse);

    return decodedPrefix(parsedReference, resolvedWarehouse);
  }

  static Branch checkBranch(Reference reference) {
    checkArgument(
        reference instanceof Branch, "Can only commit against a branch, but got " + reference);
    return (Branch) reference;
  }

  static Map<String, String> createEntityProperties(Map<String, String> providedProperties) {
    Map<String, String> properties = new HashMap<>();
    properties.put("created-at", OffsetDateTime.now(ZoneOffset.UTC).toString());
    providedProperties.forEach(
        (k, v) -> {
          if (!DERIVED_PROPERTIES.contains(k)) {
            properties.put(k, v);
          }
        });
    return properties;
  }

  WarehouseConfig createEntityCommonOps(
      TableRef tableRef, Content.Type type, List<IcebergMetadataUpdate> updates)
      throws NessieNotFoundException, CatalogEntityAlreadyExistsException {
    ParsedReference ref = requireNonNull(tableRef.reference());

    GetMultipleContentsResponse contentResponse =
        contentService.getMultipleContents(
            ref.name(),
            ref.hashWithRelativeSpec(),
            List.of(tableRef.contentKey()),
            false,
            API_WRITE);
    if (!contentResponse.getContents().isEmpty()) {
      Content existing = contentResponse.getContents().get(0).getContent();
      throw new CatalogEntityAlreadyExistsException(
          false, type, tableRef.contentKey(), existing.getType());
    }
    checkBranch(contentResponse.getEffectiveReference());

    WarehouseConfig warehouse = lakehouseConfig.catalog().getWarehouse(tableRef.warehouse());
    String location =
        icebergNewEntityBaseLocation(
            catalogService
                .locationForEntity(
                    warehouse,
                    tableRef.contentKey(),
                    ICEBERG_TABLE,
                    ICEBERG_V1,
                    ref.name(),
                    ref.hashWithRelativeSpec())
                .toString());
    updates.add(setTrustedLocation(location));

    return warehouse;
  }

  ContentResponse fetchIcebergEntity(
      TableRef tableRef,
      Content.Type expectedType,
      String expectedTypeName,
      boolean forWrite,
      boolean notFoundIfWrongType)
      throws NessieNotFoundException {
    ParsedReference ref = requireNonNull(tableRef.reference());
    ContentResponse content =
        contentService.getContent(
            tableRef.contentKey(),
            ref.name(),
            ref.hashWithRelativeSpec(),
            false,
            forWrite ? API_WRITE : API_READ);
    boolean correctType = content.getContent().getType().equals(expectedType);
    if (notFoundIfWrongType && !correctType) {
      throw new NessieContentNotFoundException(tableRef.contentKey(), ref.name());
    }
    checkArgument(
        correctType,
        "Expecting an Iceberg %s, but got type %s",
        expectedTypeName,
        content.getContent().getType());
    return content;
  }

  Uni<SnapshotResponse> createOrUpdateEntity(
      TableRef tableRef,
      IcebergUpdateEntityRequest updateEntityRequest,
      Content.Type contentType,
      CatalogOps apiOperation)
      throws IOException {

    IcebergCatalogOperation op =
        IcebergCatalogOperation.builder()
            .updates(updateEntityRequest.updates())
            .requirements(updateEntityRequest.requirements())
            .key(tableRef.contentKey())
            .warehouse(tableRef.warehouse())
            .type(contentType)
            .build();

    CatalogCommit commit = CatalogCommit.builder().addOperations(op).build();

    SnapshotReqParams reqParams =
        SnapshotReqParams.forSnapshotHttpReq(tableRef.reference(), "iceberg", null);

    return Uni.createFrom()
        .completionStage(
            catalogService.commit(
                tableRef.reference(),
                commit,
                reqParams,
                this::updateCommitMeta,
                apiOperation.name(),
                ICEBERG_V1))
        .map(Stream::findFirst)
        .map(
            o ->
                o.orElseThrow(
                    () -> new IllegalStateException("Catalog commit returned no response")));
  }
}

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
import static org.projectnessie.api.v2.params.ParsedReference.parsedReference;
import static org.projectnessie.api.v2.params.ReferenceResolver.resolveReferencePathElement;
import static org.projectnessie.catalog.formats.iceberg.nessie.NessieModelIceberg.typeToEntityName;
import static org.projectnessie.catalog.service.rest.DecodedPrefix.decodedPrefix;
import static org.projectnessie.catalog.service.rest.NamespaceRef.namespaceRef;
import static org.projectnessie.catalog.service.rest.TableRef.tableRef;
import static org.projectnessie.model.CommitMeta.fromMessage;
import static org.projectnessie.model.Reference.ReferenceType.BRANCH;

import com.google.common.base.Splitter;
import jakarta.inject.Inject;
import java.net.URI;
import java.util.Locale;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Stream;
import org.projectnessie.api.v2.params.ParsedReference;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergTableIdentifier;
import org.projectnessie.catalog.formats.iceberg.rest.IcebergRenameTableRequest;
import org.projectnessie.catalog.service.api.CatalogEntityAlreadyExistsException;
import org.projectnessie.catalog.service.api.CatalogService;
import org.projectnessie.catalog.service.api.SnapshotResponse;
import org.projectnessie.catalog.service.config.CatalogConfig;
import org.projectnessie.client.api.NessieApiV2;
import org.projectnessie.client.api.PagingBuilder;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieContentNotFoundException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Branch;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.EntriesResponse;
import org.projectnessie.model.GetMultipleContentsResponse;
import org.projectnessie.model.Namespace;
import org.projectnessie.model.Operation;
import org.projectnessie.model.Reference;
import org.projectnessie.model.TableReference;
import org.projectnessie.services.config.ServerConfig;

abstract class IcebergApiV1ResourceBase extends AbstractCatalogResource {

  @Inject NessieApiV2 nessieApi;
  @Inject ServerConfig serverConfig;
  @Inject CatalogConfig catalogConfig;

  protected Stream<EntriesResponse.Entry> listContent(
      NamespaceRef namespaceRef,
      String pageToken,
      Integer pageSize,
      boolean withContent,
      String celFilter,
      Consumer<String> responsePagingToken)
      throws NessieNotFoundException {

    EntriesResponse entriesResponse =
        applyPaging(
                nessieApi
                    .getEntries()
                    .refName(namespaceRef.referenceName())
                    .hashOnRef(namespaceRef.hashWithRelativeSpec())
                    .filter(celFilter)
                    .withContent(withContent),
                pageToken,
                pageSize)
            .get();

    String token = entriesResponse.getToken();
    if (token != null) {
      responsePagingToken.accept(token);
    }

    return entriesResponse.getEntries().stream();
  }

  private static <P extends PagingBuilder<?, ?, ?>> P applyPaging(
      P pageable, String pageToken, Integer pageSize) {
    if (pageSize != null) {
      if (pageToken != null) {
        pageable.pageToken(pageToken);
      }
      pageable.maxRecords(pageSize);
    }

    return pageable;
  }

  protected void renameContent(
      String prefix, IcebergRenameTableRequest renameTableRequest, Content.Type expectedContentType)
      throws NessieNotFoundException, NessieConflictException {
    TableRef fromTableRef = decodeTableRef(prefix, renameTableRequest.source());
    TableRef toTableRef = decodeTableRef(prefix, renameTableRequest.destination());

    GetMultipleContentsResponse contents =
        nessieApi
            .getContent()
            .refName(fromTableRef.reference().name())
            .hashOnRef(fromTableRef.reference().hashWithRelativeSpec())
            .key(toTableRef.contentKey())
            .key(fromTableRef.contentKey())
            .getWithResponse();
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

    nessieApi
        .commitMultipleOperations()
        .branch((Branch) effectiveRef)
        .commitMeta(
            fromMessage(
                format(
                    "rename %s %s to %s",
                    entityType, fromTableRef.contentKey(), toTableRef.contentKey())))
        .operation(Operation.Delete.of(fromTableRef.contentKey()))
        .operation(Operation.Put.of(toTableRef.contentKey(), existingFrom))
        .commitWithResponse();
  }

  protected NamespaceRef decodeNamespaceRef(String prefix, String encodedNs) {
    DecodedPrefix decoded = decodePrefix(prefix);
    ParsedReference ref = decoded.parsedReference();
    Namespace ns = decodeNamespace(encodedNs);
    return namespaceRef(ns, ref.name(), ref.hashWithRelativeSpec(), decoded.warehouse());
  }

  public TableRef decodeTableRefWithHash(String prefix, String encodedNs, String table)
      throws NessieNotFoundException {
    TableRef tableRef = decodeTableRef(prefix, encodedNs, table);

    ParsedReference reference = tableRef.reference();
    if (reference.hashWithRelativeSpec() == null) {
      Reference ref = nessieApi.getReference().refName(reference.name()).get();
      reference = ParsedReference.parsedReference(ref.getName(), ref.getHash(), ref.getType());
      return tableRef(tableRef.contentKey(), reference, tableRef.warehouse());
    }

    return tableRef;
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
    String refName =
        tableReference.getReference() != null ? tableReference.getReference() : ref.name();
    String refHash =
        tableReference.getHash() != null ? tableReference.getHash() : ref.hashWithRelativeSpec();
    return tableRef(contentKey, parsedReference(refName, refHash, null), decoded.warehouse());
  }

  public static Namespace decodeNamespace(String encodedNs) {
    if (encodedNs == null) {
      return Namespace.EMPTY;
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

    String resolvedWarehouse = catalogConfig.resolveWarehouseName(warehouse);

    return decodedPrefix(parsedReference, resolvedWarehouse);
  }

  static Branch checkBranch(Reference reference) {
    checkArgument(
        reference instanceof Branch, "Can only commit against a branch, but got " + reference);
    return (Branch) reference;
  }

  protected String snapshotMetadataLocation(SnapshotResponse snap) {
    // TODO the resolved metadataLocation is wrong !!
    CatalogService.CatalogUriResolver catalogUriResolver = new CatalogUriResolverImpl(uriInfo);
    URI metadataLocation =
        catalogUriResolver.icebergSnapshot(
            snap.effectiveReference(), snap.contentKey(), snap.nessieSnapshot());
    return metadataLocation.toString();
  }
}

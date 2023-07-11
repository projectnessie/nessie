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
package org.projectnessie.catalog.service.resources;

import static com.google.common.base.Preconditions.checkArgument;
import static org.projectnessie.api.v2.params.ReferenceResolver.resolveReferencePathElement;
import static org.projectnessie.catalog.service.spi.DecodedPrefix.decodedPrefix;
import static org.projectnessie.catalog.service.spi.NamespaceRef.namespaceRef;
import static org.projectnessie.catalog.service.spi.TableRef.tableRef;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Splitter;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableSet;
import java.util.Set;
import javax.inject.Inject;
import org.apache.iceberg.catalog.TableIdentifier;
import org.projectnessie.api.v2.params.ParsedReference;
import org.projectnessie.catalog.service.spi.DecodedPrefix;
import org.projectnessie.catalog.service.spi.NamespaceRef;
import org.projectnessie.catalog.service.spi.TableRef;
import org.projectnessie.catalog.service.spi.TenantSpecific;
import org.projectnessie.model.Branch;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.Namespace;
import org.projectnessie.model.Reference;
import org.projectnessie.model.TableReference;

public abstract class BaseIcebergResource {

  public static final char SEPARATOR = '\u001f';

  public static final String PROPERTY_LOAD_REF = "nessie.load.ref";
  public static final String PROPERTY_LOAD_COMMIT = "nessie.load.commit";
  public static final String PROPERTY_STAGED_REF = "nessie.staged.ref";
  public static final String PROPERTY_STAGED_COMMIT = "nessie.staged.commit";
  public static final String PROPERTY_COMMIT_ID = "nessie.commit.id";

  public static final Set<String> CHANGING_PROPERTIES =
      ImmutableSet.of(
          PROPERTY_LOAD_REF, PROPERTY_LOAD_COMMIT, PROPERTY_STAGED_REF, PROPERTY_STAGED_COMMIT);

  private static final Splitter NAMESPACE_ESCAPED_SPLITTER = Splitter.on(SEPARATOR);
  private static final String DEFAULT_REF_IN_PATH = "-";

  @Inject @jakarta.inject.Inject protected TenantSpecific tenantSpecific;

  protected DecodedPrefix decodePrefix(String prefix) {
    ParsedReference parsedReference = tenantSpecific.defaultBranch();
    String warehouse = tenantSpecific.defaultWarehouse().name();

    return decodePrefix(
        prefix, parsedReference, warehouse, () -> tenantSpecific.defaultBranch().name());
  }

  @VisibleForTesting
  static DecodedPrefix decodePrefix(
      String prefix,
      ParsedReference parsedReference,
      String warehouse,
      Supplier<String> defaultBranchSupplier) {
    if (prefix != null) {
      prefix = prefix.replace(BaseIcebergResource.SEPARATOR, '/');

      int indexAt = prefix.indexOf('|');
      if (indexAt != -1) {
        if (indexAt != prefix.length() - 1) {
          warehouse = prefix.substring(indexAt + 1);
        }
        prefix = prefix.substring(0, indexAt);
      }

      if (!prefix.isEmpty() && !DEFAULT_REF_IN_PATH.equals(prefix)) {
        parsedReference = resolveReferencePathElement(prefix, null, defaultBranchSupplier);
      }
    }

    return decodedPrefix(parsedReference, warehouse);
  }

  protected NamespaceRef decodeNamespaceRef(String prefix, String encodedNs) {
    DecodedPrefix decoded = decodePrefix(prefix);
    ParsedReference ref = decoded.parsedReference();
    Namespace ns = decodeNamespace(encodedNs);
    return namespaceRef(ns, ref.name(), ref.hashWithRelativeSpec(), decoded.warehouse());
  }

  public TableRef decodeTableRef(String prefix, String encodedNs, String table) {
    Namespace ns = decodeNamespace(encodedNs);
    TableReference tableReference = TableReference.parse(table);

    return fixupTableRef(prefix, tableReference, ns);
  }

  public TableRef decodeTableRef(String prefix, TableIdentifier table) {
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
    return tableRef(contentKey, refName, refHash, decoded.warehouse());
  }

  public static Namespace decodeNamespace(String encodedNs) {
    if (encodedNs == null) {
      return Namespace.EMPTY;
    }

    return Namespace.of(NAMESPACE_ESCAPED_SPLITTER.splitToList(encodedNs).toArray(new String[0]));
  }

  org.apache.iceberg.catalog.Namespace toIcebergNamespace(Namespace namespace) {
    return org.apache.iceberg.catalog.Namespace.of(namespace.getElementsArray());
  }

  public static Namespace toNessieNamespace(org.apache.iceberg.catalog.Namespace namespace) {
    return Namespace.of(namespace.levels());
  }

  static Branch checkBranch(Reference reference) {
    checkArgument(
        reference instanceof Branch, "Can only commit against a branch, but got " + reference);
    return (Branch) reference;
  }

  static IcebergTable verifyIcebergTable(Content content) {
    checkArgument(
        content instanceof IcebergTable,
        "Table identifier references a content of type %s, not an Iceberg table",
        content.getType().name());
    return (IcebergTable) content;
  }
}

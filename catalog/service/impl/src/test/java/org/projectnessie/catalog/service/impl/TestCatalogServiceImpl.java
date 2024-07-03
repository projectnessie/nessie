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

import static org.projectnessie.api.v2.params.ParsedReference.parsedReference;
import static org.projectnessie.catalog.formats.iceberg.rest.IcebergMetadataUpdate.AddPartitionSpec.addPartitionSpec;
import static org.projectnessie.catalog.formats.iceberg.rest.IcebergMetadataUpdate.AddSchema.addSchema;
import static org.projectnessie.catalog.formats.iceberg.rest.IcebergMetadataUpdate.AddSortOrder.addSortOrder;
import static org.projectnessie.catalog.formats.iceberg.rest.IcebergMetadataUpdate.AssignUUID.assignUUID;
import static org.projectnessie.catalog.formats.iceberg.rest.IcebergMetadataUpdate.SetCurrentSchema.setCurrentSchema;
import static org.projectnessie.catalog.formats.iceberg.rest.IcebergMetadataUpdate.SetDefaultPartitionSpec.setDefaultPartitionSpec;
import static org.projectnessie.catalog.formats.iceberg.rest.IcebergMetadataUpdate.SetDefaultSortOrder.setDefaultSortOrder;
import static org.projectnessie.catalog.formats.iceberg.rest.IcebergMetadataUpdate.UpgradeFormatVersion.upgradeFormatVersion;
import static org.projectnessie.catalog.formats.iceberg.rest.IcebergUpdateRequirement.AssertCreate.assertTableDoesNotExist;
import static org.projectnessie.model.Content.Type.ICEBERG_TABLE;

import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.projectnessie.api.v2.params.ParsedReference;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergPartitionSpec;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergSchema;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergSortOrder;
import org.projectnessie.catalog.formats.iceberg.rest.IcebergCatalogOperation;
import org.projectnessie.catalog.service.api.CatalogCommit;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.Reference;

public class TestCatalogServiceImpl extends AbstractCatalogService {

  @Test
  public void singleTableCreate() throws Exception {
    Reference main = api.getReference().refName("main").get();

    ParsedReference ref =
        parsedReference(main.getName(), main.getHash(), Reference.ReferenceType.BRANCH);
    ContentKey key = ContentKey.of("mytable");
    CatalogCommit commit =
        CatalogCommit.builder()
            .addOperations(
                IcebergCatalogOperation.builder()
                    .key(key)
                    .addUpdates(
                        assignUUID(UUID.randomUUID().toString()),
                        upgradeFormatVersion(2),
                        addSchema(IcebergSchema.builder().build(), 0),
                        setCurrentSchema(-1),
                        addPartitionSpec(IcebergPartitionSpec.UNPARTITIONED_SPEC),
                        setDefaultPartitionSpec(-1),
                        addSortOrder(IcebergSortOrder.UNSORTED_ORDER),
                        setDefaultSortOrder(-1))
                    .addRequirement(assertTableDoesNotExist())
                    .type(ICEBERG_TABLE)
                    .build())
            .build();

    catalogService.commit(ref, commit).toCompletableFuture().get();

    Reference afterCommit = api.getReference().refName("main").get();
    soft.assertThat(afterCommit).isNotEqualTo(main);
  }
}

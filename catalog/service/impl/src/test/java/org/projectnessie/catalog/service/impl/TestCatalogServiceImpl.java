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

import org.junit.jupiter.api.Test;
import org.projectnessie.api.v2.params.ParsedReference;
import org.projectnessie.catalog.service.api.CatalogCommit;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.Reference;

public class TestCatalogServiceImpl extends AbstractCatalogService {

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
  }
}

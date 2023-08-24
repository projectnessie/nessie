/*
 * Copyright (C) 2022 Dremio
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
package org.projectnessie.quarkus.cli;

import java.util.UUID;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.Namespace;
import org.projectnessie.nessie.relocated.protobuf.ByteString;
import org.projectnessie.versioned.BranchName;
import org.projectnessie.versioned.CommitMetaSerializer;
import org.projectnessie.versioned.GetNamedRefsParams;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.ReferenceInfo;
import org.projectnessie.versioned.ReferenceNotFoundException;
import org.projectnessie.versioned.persist.adapter.ContentId;
import org.projectnessie.versioned.persist.adapter.DatabaseAdapter;
import org.projectnessie.versioned.persist.adapter.ImmutableCommitParams;
import org.projectnessie.versioned.persist.adapter.KeyWithBytes;
import org.projectnessie.versioned.store.DefaultStoreWorker;

abstract class BaseContentDatabaseAdapterTest<OutputType> extends BaseContentTest<OutputType> {

  private final DatabaseAdapter adapter;

  BaseContentDatabaseAdapterTest(DatabaseAdapter adapter, Class<OutputType> outputClass) {
    super(outputClass);
    this.adapter = adapter;
  }

  @Override
  protected void commit(
      ContentKey key, String contentId, byte payload, ByteString value, boolean createNamespace)
      throws Exception {
    ContentId id = ContentId.of(contentId);

    ImmutableCommitParams.Builder c =
        ImmutableCommitParams.builder()
            .toBranch(BranchName.of("main"))
            .commitMetaSerialized(
                CommitMetaSerializer.METADATA_SERIALIZER.toBytes(
                    CommitMeta.fromMessage(contentId)));

    if (createNamespace && !testNamespaceCreated) {
      namespace =
          Namespace.builder()
              .id(UUID.randomUUID().toString())
              .addElements("test_namespace")
              .build();

      ByteString namespaceValue = DefaultStoreWorker.instance().toStoreOnReferenceState(namespace);
      byte nsPayload = (byte) DefaultStoreWorker.payloadForContent(namespace);
      c.addPuts(
          KeyWithBytes.of(
              ContentKey.of("test_namespace"),
              ContentId.of(namespace.getId()),
              nsPayload,
              namespaceValue));
      testNamespaceCreated = true;
    }

    c.addPuts(KeyWithBytes.of(key, id, payload, value));

    adapter.commit(c.build());
  }

  @Override
  protected Hash getMainHead() throws ReferenceNotFoundException {
    ReferenceInfo<ByteString> main = adapter.namedRef("main", GetNamedRefsParams.DEFAULT);
    return main.getHash();
  }
}

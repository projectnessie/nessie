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

import static org.projectnessie.versioned.storage.common.logic.CreateCommit.Add.commitAdd;
import static org.projectnessie.versioned.storage.common.logic.CreateCommit.newCommitBuilder;
import static org.projectnessie.versioned.storage.common.logic.Logics.commitLogic;
import static org.projectnessie.versioned.storage.common.logic.Logics.referenceLogic;
import static org.projectnessie.versioned.storage.common.objtypes.CommitHeaders.EMPTY_COMMIT_HEADERS;
import static org.projectnessie.versioned.storage.common.objtypes.ContentValueObj.contentValue;
import static org.projectnessie.versioned.storage.versionstore.TypeMapping.keyToStoreKey;
import static org.projectnessie.versioned.storage.versionstore.TypeMapping.objIdToHash;

import java.util.Collections;
import java.util.Objects;
import java.util.UUID;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.Namespace;
import org.projectnessie.nessie.relocated.protobuf.ByteString;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.storage.common.logic.CommitLogic;
import org.projectnessie.versioned.storage.common.logic.CreateCommit.Builder;
import org.projectnessie.versioned.storage.common.logic.CreateCommit.Remove;
import org.projectnessie.versioned.storage.common.objtypes.CommitObj;
import org.projectnessie.versioned.storage.common.objtypes.ContentValueObj;
import org.projectnessie.versioned.storage.common.persist.Persist;
import org.projectnessie.versioned.storage.common.persist.Reference;
import org.projectnessie.versioned.store.DefaultStoreWorker;

abstract class BaseContentPersistTest<OutputType> extends BaseContentTest<OutputType> {

  private final Persist persist;

  BaseContentPersistTest(Persist persist, Class<OutputType> outputClass) {
    super(outputClass);
    this.persist = persist;
  }

  @Override
  protected void commit(
      ContentKey key,
      UUID contentId,
      byte payload,
      ByteString value,
      boolean createNamespace,
      boolean add)
      throws Exception {

    Reference refMain = referenceLogic(persist).getReference("refs/heads/main");
    Builder builder =
        newCommitBuilder()
            .parentCommitId(refMain.pointer())
            .message(contentId.toString())
            .headers(EMPTY_COMMIT_HEADERS);

    if (createNamespace && !testNamespaceCreated) {
      namespace =
          Namespace.builder()
              .id(UUID.randomUUID().toString())
              .addElements("test_namespace")
              .build();

      ByteString namespaceValue = DefaultStoreWorker.instance().toStoreOnReferenceState(namespace);
      byte nsPayload = (byte) DefaultStoreWorker.payloadForContent(namespace);
      ContentValueObj nsValueObj = contentValue(namespace.getId(), nsPayload, namespaceValue);
      persist.storeObj(nsValueObj);

      ContentKey nsKey = ContentKey.of("test_namespace");
      builder.addAdds(
          commitAdd(keyToStoreKey(nsKey), nsPayload, nsValueObj.id(), null, UUID.randomUUID()));

      testNamespaceCreated = true;
    }

    ContentValueObj valueObj = contentValue(contentId.toString(), payload, value);
    if (add) {
      persist.storeObj(valueObj);
      // Note: cannot PUT an existing value for now (missing expected value)
      builder.addAdds(commitAdd(keyToStoreKey(key), payload, valueObj.id(), null, contentId));
    } else {
      builder.addRemoves(
          Remove.commitRemove(keyToStoreKey(key), payload, valueObj.id(), contentId));
    }

    CommitLogic commitLogic = commitLogic(persist);
    CommitObj commit = commitLogic.doCommit(builder.build(), Collections.emptyList());

    referenceLogic(persist).assignReference(refMain, commit.id());
  }

  @Override
  protected Hash getMainHead() {
    Reference main = persist.fetchReference("refs/heads/main");
    return objIdToHash(Objects.requireNonNull(main).pointer());
  }
}

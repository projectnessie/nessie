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
import static org.projectnessie.versioned.store.DefaultStoreWorker.payloadForContent;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.test.junit.main.LaunchResult;
import io.quarkus.test.junit.main.QuarkusMainLauncher;
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.io.TempDir;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.Namespace;
import org.projectnessie.nessie.relocated.protobuf.ByteString;
import org.projectnessie.versioned.storage.common.logic.CommitLogic;
import org.projectnessie.versioned.storage.common.logic.CreateCommit.Builder;
import org.projectnessie.versioned.storage.common.objtypes.CommitObj;
import org.projectnessie.versioned.storage.common.objtypes.ContentValueObj;
import org.projectnessie.versioned.storage.common.persist.Persist;
import org.projectnessie.versioned.storage.common.persist.Reference;
import org.projectnessie.versioned.store.DefaultStoreWorker;

abstract class BaseContentPersistTest<OutputType> {
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private final Class<OutputType> outputClass;

  @TempDir File tempDir;

  protected LaunchResult result;
  protected List<OutputType> entries;

  private boolean testNamespaceCreated;

  protected Namespace namespace;

  BaseContentPersistTest(Class<OutputType> outputClass) {
    this.outputClass = outputClass;
  }

  protected void launchNoFile(QuarkusMainLauncher launcher, String... args) {
    launch(launcher, null, args);
  }

  protected void launch(QuarkusMainLauncher launcher, String... args) throws Exception {
    File output = new File(tempDir, "check-content.json");
    launch(launcher, output, args);
    JavaType type = MAPPER.getTypeFactory().constructCollectionType(List.class, outputClass);
    entries = MAPPER.readValue(output, type);
  }

  private void launch(QuarkusMainLauncher launcher, File outputFile, String... args) {
    List<String> cmdArgs = new ArrayList<>(Arrays.asList(args));

    if (outputFile != null) {
      cmdArgs.add("--output");
      cmdArgs.add(outputFile.getAbsolutePath());
    }

    result = launcher.launch(cmdArgs.toArray(new String[0]));
  }

  protected void commit(IcebergTable table, Persist persist) throws Exception {
    commit(table, DefaultStoreWorker.instance().toStoreOnReferenceState(table), persist);
  }

  protected void commit(IcebergTable table, ByteString serialized, Persist persist)
      throws Exception {
    commit(
        ContentKey.of("test_namespace", "table_" + table.getId()),
        table.getId(),
        (byte) payloadForContent(table),
        serialized,
        true,
        persist);
  }

  protected void commit(
      ContentKey key,
      String contentId,
      byte payload,
      ByteString value,
      boolean createNamespace,
      Persist persist)
      throws Exception {

    Reference refMain = referenceLogic(persist).getReference("refs/heads/main");
    Builder builder =
        newCommitBuilder()
            .parentCommitId(refMain.pointer())
            .message(contentId)
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

    ContentValueObj valueObj = contentValue(contentId, payload, value);
    persist.storeObj(valueObj);

    builder.addAdds(commitAdd(keyToStoreKey(key), payload, valueObj.id(), null, UUID.randomUUID()));

    CommitLogic commitLogic = commitLogic(persist);
    CommitObj commit = commitLogic.doCommit(builder.build(), Collections.emptyList());

    referenceLogic(persist).assignReference(refMain, commit.id());
  }
}

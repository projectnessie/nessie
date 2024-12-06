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
package org.projectnessie.tools.admin.cli;

import static org.projectnessie.versioned.storage.common.logic.CreateCommit.Add.commitAdd;
import static org.projectnessie.versioned.storage.common.logic.CreateCommit.newCommitBuilder;
import static org.projectnessie.versioned.storage.common.logic.Logics.commitLogic;
import static org.projectnessie.versioned.storage.common.logic.Logics.referenceLogic;
import static org.projectnessie.versioned.storage.common.objtypes.CommitHeaders.EMPTY_COMMIT_HEADERS;
import static org.projectnessie.versioned.storage.common.objtypes.ContentValueObj.contentValue;
import static org.projectnessie.versioned.storage.versionstore.TypeMapping.keyToStoreKey;
import static org.projectnessie.versioned.storage.versionstore.TypeMapping.objIdToHash;
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
import java.util.Objects;
import java.util.UUID;
import org.junit.jupiter.api.io.TempDir;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.IcebergTable;
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

abstract class AbstractContentTests<OutputType> {
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private final Persist persist;
  private final Class<OutputType> outputClass;

  @TempDir File tempDir;

  protected LaunchResult result;
  protected List<OutputType> entries;

  protected boolean testNamespaceCreated;

  protected Namespace namespace;

  AbstractContentTests(Persist persist, Class<OutputType> outputClass) {
    this.persist = persist;
    this.outputClass = outputClass;
  }

  protected Persist persist() {
    return persist;
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

  protected CommitObj commit(IcebergTable table) throws Exception {
    return commit(table, true);
  }

  protected CommitObj commit(Content table, boolean add) throws Exception {
    return commit(table, ContentKey.of("test_namespace", "table_" + table.getId()), add);
  }

  protected CommitObj commit(Content table, ContentKey key, boolean add) throws Exception {
    ByteString serialized = DefaultStoreWorker.instance().toStoreOnReferenceState(table);
    return commit(
        key,
        UUID.fromString(Objects.requireNonNull(table.getId())),
        (byte) payloadForContent(table),
        serialized,
        true,
        add);
  }

  protected void commit(IcebergTable table, ByteString serialized) throws Exception {
    commit(
        ContentKey.of("test_namespace", "table_" + table.getId()),
        UUID.fromString(Objects.requireNonNull(table.getId())),
        (byte) payloadForContent(table),
        serialized,
        true,
        true);
  }

  protected CommitObj commit(
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
    CommitObj commit =
        Objects.requireNonNull(commitLogic.doCommit(builder.build(), Collections.emptyList()));

    referenceLogic(persist).assignReference(refMain, commit.id());
    return commit;
  }

  protected Hash getMainHead() {
    Reference main = persist.fetchReference("refs/heads/main");
    return objIdToHash(Objects.requireNonNull(main).pointer());
  }
}

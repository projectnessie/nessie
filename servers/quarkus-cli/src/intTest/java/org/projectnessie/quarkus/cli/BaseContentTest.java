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

import static org.projectnessie.versioned.store.DefaultStoreWorker.payloadForContent;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.ByteString;
import io.quarkus.test.junit.main.LaunchResult;
import io.quarkus.test.junit.main.QuarkusMainLauncher;
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.io.TempDir;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.Namespace;
import org.projectnessie.versioned.BranchName;
import org.projectnessie.versioned.CommitMetaSerializer;
import org.projectnessie.versioned.persist.adapter.ContentId;
import org.projectnessie.versioned.persist.adapter.DatabaseAdapter;
import org.projectnessie.versioned.persist.adapter.ImmutableCommitParams;
import org.projectnessie.versioned.persist.adapter.KeyWithBytes;
import org.projectnessie.versioned.store.DefaultStoreWorker;

abstract class BaseContentTest<OutputType> {
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private final Class<OutputType> outputClass;

  @TempDir File tempDir;

  protected LaunchResult result;
  protected List<OutputType> entries;

  BaseContentTest(Class<OutputType> outputClass) {
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

  protected void commit(IcebergTable table, DatabaseAdapter adapter) throws Exception {
    commit(table, adapter, DefaultStoreWorker.instance().toStoreOnReferenceState(table, att -> {}));
  }

  protected void commit(IcebergTable table, DatabaseAdapter adapter, ByteString serialized)
      throws Exception {
    commit(table.getId(), payloadForContent(table), serialized, adapter);
  }

  private boolean testNamespaceCreated;
  protected Namespace namespace;

  protected void commit(String testId, byte payload, ByteString value, DatabaseAdapter adapter)
      throws Exception {
    ContentKey key = ContentKey.of("test_namespace", "table_" + testId);
    ContentId id = ContentId.of(testId);

    ImmutableCommitParams.Builder c =
        ImmutableCommitParams.builder()
            .toBranch(BranchName.of("main"))
            .commitMetaSerialized(
                CommitMetaSerializer.METADATA_SERIALIZER.toBytes(CommitMeta.fromMessage(testId)));

    if (!testNamespaceCreated) {
      namespace =
          Namespace.builder()
              .id(UUID.randomUUID().toString())
              .addElements("test_namespace")
              .build();
      ByteString namespaceValue =
          DefaultStoreWorker.instance().toStoreOnReferenceState(namespace, x -> {});
      byte nsPayload = DefaultStoreWorker.payloadForContent(namespace);
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
}

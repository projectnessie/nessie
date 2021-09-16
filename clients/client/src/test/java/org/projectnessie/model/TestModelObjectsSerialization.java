/*
 * Copyright (C) 2020 Dremio
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
package org.projectnessie.model;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Instant;
import org.junit.jupiter.api.Test;
import org.projectnessie.model.Contents.Type;
import org.projectnessie.model.EntriesResponse.Entry;

/**
 * This test merely checks the JSON serialization/deserialization of the model classes, with an
 * intention to identify breaking cases whenever jackson version varies.
 */
public class TestModelObjectsSerialization {

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final Instant NOW = Instant.now();
  private static final String TEST_HASH =
      "3e1cfa82b035c26cbbbdae632cea070514eb8b773f616aaeaf668e2f0be8f10e";

  @Test
  public void testBranchSerDe() throws JsonProcessingException {
    String name = "testbranch";
    Branch branchWithNameAndHash = ImmutableBranch.builder().hash(TEST_HASH).name(name).build();
    Branch branchWithNameOnly = ImmutableBranch.builder().name(name).build();

    assertBranchSerDe(branchWithNameAndHash);
    assertBranchSerDe(branchWithNameOnly);
  }

  private void assertBranchSerDe(Branch branch) throws JsonProcessingException {
    String serializedJson = MAPPER.writeValueAsString(branch);
    Branch branchDeSerialized = MAPPER.readValue(serializedJson, Branch.class);
    assertEquals(branch, branchDeSerialized);
    Reference refDeSerialized = MAPPER.readValue(serializedJson, Reference.class);
    assertEquals(branch, refDeSerialized);
  }

  @Test
  public void testEntriesResponseSerDe() throws JsonProcessingException {
    Entry entry =
        ImmutableEntry.builder()
            .type(Type.ICEBERG_TABLE)
            .name(ContentsKey.fromPathString("/tmp/testpath"))
            .build();
    EntriesResponse entriesResponse =
        ImmutableEntriesResponse.builder().addEntries(entry).token(TEST_HASH).hasMore(true).build();

    String entriesResponseJson = MAPPER.writeValueAsString(entriesResponse);
    EntriesResponse entriesResponseDeserialized =
        MAPPER.readValue(entriesResponseJson, EntriesResponse.class);
    assertEquals(entriesResponse, entriesResponseDeserialized);
  }

  @Test
  public void testLogResponseSerDe() throws JsonProcessingException {
    CommitMeta testCommit =
        ImmutableCommitMeta.builder()
            .commitTime(NOW)
            .author("author@testnessie.com")
            .committer("committer@testnessie.com")
            .authorTime(NOW)
            .hash(TEST_HASH)
            .message("test commit")
            .putProperties("prop1", "val1")
            .signedOffBy("signer@testnessie.com")
            .build();

    LogResponse logResponse =
        ImmutableLogResponse.builder()
            .addOperations()
            .token(TEST_HASH)
            .addOperations(testCommit)
            .hasMore(true)
            .build();
    String logResponseJson = MAPPER.writeValueAsString(logResponse);
    LogResponse logResponseDeserialized = MAPPER.readValue(logResponseJson, LogResponse.class);
    assertEquals(logResponse, logResponseDeserialized);
  }

  @Test
  public void testMergeSerDe() throws JsonProcessingException {
    Merge merge = ImmutableMerge.builder().fromHash(TEST_HASH).fromRefName("ref1").build();
    String mergeJson = MAPPER.writeValueAsString(merge);
    Merge mergeDeserialized = MAPPER.readValue(mergeJson, Merge.class);
    assertEquals(merge, mergeDeserialized);
  }

  @Test
  public void testTransplantSerDe() throws JsonProcessingException {
    Transplant transplant =
        ImmutableTransplant.builder()
            .addHashesToTransplant(TEST_HASH)
            .fromRefName("testref")
            .build();
    String transplantJson = MAPPER.writeValueAsString(transplant);
    Transplant transplantDeserialized = MAPPER.readValue(transplantJson, Transplant.class);
    assertEquals(transplant, transplantDeserialized);
  }
}

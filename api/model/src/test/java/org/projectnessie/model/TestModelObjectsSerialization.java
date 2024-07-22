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
package org.projectnessie.model;

import static org.projectnessie.model.Conflict.ConflictType.UNEXPECTED_HASH;
import static org.projectnessie.model.Conflict.conflict;
import static org.projectnessie.model.JsonUtil.arrayNode;
import static org.projectnessie.model.JsonUtil.objectNode;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.projectnessie.error.ReferenceConflicts;
import org.projectnessie.model.LogResponse.LogEntry;
import org.projectnessie.model.ser.Views;

/**
 * This test merely checks the JSON serialization/deserialization of the model classes, with an
 * intention to identify breaking cases whenever jackson version varies.
 */
public class TestModelObjectsSerialization {

  private static final ObjectMapper MAPPER = new ObjectMapper();
  protected static final String HASH =
      "3e1cfa82b035c26cbbbdae632cea070514eb8b773f616aaeaf668e2f0be8f10e";

  @ParameterizedTest
  @MethodSource("goodCases")
  void testGoodSerDeCases(Case goodCase) throws IOException {
    String json =
        MAPPER.writerWithView(goodCase.serializationView).writeValueAsString(goodCase.obj);
    JsonNode j = MAPPER.readValue(json, JsonNode.class);
    Assertions.assertThat(j).isEqualTo(goodCase.deserializedJson);
    Object deserialized = MAPPER.readValue(json, goodCase.deserializeAs);
    if (!goodCase.skipFinalCompare) {
      Assertions.assertThat(deserialized).isEqualTo(goodCase.obj);
    }
  }

  @ParameterizedTest
  @MethodSource("negativeCases")
  void testNegativeSerDeCases(Case invalidCase) {
    Assertions.assertThatThrownBy(
            () ->
                MAPPER.readValue(
                    invalidCase.deserializedJson.toString(), invalidCase.deserializeAs))
        .isInstanceOf(JsonProcessingException.class);
  }

  static List<Case> goodCases() {
    final Instant now = Instant.now();
    final String branchName = "testBranch";

    return Arrays.asList(
        new Case(IcebergTable.class)
            .obj(IcebergTable.of("meta-mine", 1, 2, 3, 4, "cid"))
            .jsonNode(
                o ->
                    o.put("type", "ICEBERG_TABLE")
                        .put("id", "cid")
                        .put("metadataLocation", "meta-mine")
                        .put("snapshotId", 1)
                        .put("schemaId", 2)
                        .put("specId", 3)
                        .put("sortOrderId", 4)),
        new Case(IcebergView.class)
            .obj(IcebergView.of("cid", "meta-mine", 1, 2))
            .jsonNode(
                o ->
                    o.put("type", "ICEBERG_VIEW")
                        .put("id", "cid")
                        .put("metadataLocation", "meta-mine")
                        .put("versionId", 1)
                        .put("schemaId", 2)),
        new Case(UDF.class)
            .obj(
                UDF.builder()
                    .id("cid")
                    .metadataLocation("meta-mine")
                    .versionId("ver")
                    .signatureId("sig")
                    .build())
            .jsonNode(
                o ->
                    o.put("type", "UDF")
                        .put("id", "cid")
                        .put("metadataLocation", "meta-mine")
                        .put("versionId", "ver")
                        .put("signatureId", "sig")),
        new Case(ReferenceConflicts.class)
            .obj(
                ReferenceConflicts.referenceConflicts(
                    conflict(UNEXPECTED_HASH, ContentKey.of("foo", "bar"), "some message")))
            .jsonNode(
                o ->
                    o.put("type", "REFERENCE_CONFLICTS")
                        .set(
                            "conflicts",
                            arrayNode()
                                .add(
                                    objectNode()
                                        .put("conflictType", "UNEXPECTED_HASH")
                                        .put("message", "some message")
                                        .set(
                                            "key",
                                            objectNode()
                                                .set(
                                                    "elements",
                                                    arrayNode().add("foo").add("bar"))))))
            .skipFinalCompare(),
        new Case(NessieConfiguration.class)
            .view(Views.V1.class)
            .obj(
                ImmutableNessieConfiguration.builder()
                    .defaultBranch("default-branch")
                    .minSupportedApiVersion(11)
                    .maxSupportedApiVersion(42)
                    .specVersion("42.1.2")
                    .build())
            .jsonNode(
                o -> o.put("defaultBranch", "default-branch").put("maxSupportedApiVersion", 42))
            .skipFinalCompare(),
        new Case(NessieConfiguration.class)
            .view(Views.V2.class)
            .obj(
                ImmutableNessieConfiguration.builder()
                    .defaultBranch("default-branch")
                    .minSupportedApiVersion(11)
                    .maxSupportedApiVersion(42)
                    .actualApiVersion(42)
                    .specVersion("42.1.2")
                    .build())
            .jsonNode(
                o ->
                    o.put("defaultBranch", "default-branch")
                        .put("minSupportedApiVersion", 11)
                        .put("maxSupportedApiVersion", 42)
                        .put("actualApiVersion", 42)
                        .put("specVersion", "42.1.2")),
        new Case(Branch.class)
            .obj(Branch.of(branchName, HASH))
            .jsonNode(o -> o.put("type", "BRANCH").put("name", "testBranch").put("hash", HASH)),
        new Case(Branch.class)
            .obj(Branch.of(branchName, null))
            .jsonNode(o -> o.put("type", "BRANCH").put("name", "testBranch").putNull("hash")),
        new Case(Tag.class)
            .obj(Tag.of("tagname", HASH))
            .jsonNode(o -> o.put("type", "TAG").put("name", "tagname").put("hash", HASH)),
        new Case(EntriesResponse.class)
            .obj(
                EntriesResponse.builder()
                    .addEntries(
                        ImmutableEntry.builder()
                            .type(Content.Type.ICEBERG_TABLE)
                            .name(ContentKey.fromPathString("/tmp/testpath"))
                            .build())
                    .token(HASH)
                    .isHasMore(true)
                    .build())
            .jsonNode(
                o ->
                    o.put("token", HASH)
                        .put("hasMore", true)
                        .putNull("effectiveReference")
                        .putArray("entries")
                        .add(
                            objectNode()
                                .put("type", "ICEBERG_TABLE")
                                .putNull("contentId")
                                .putNull("content")
                                .set(
                                    "name",
                                    objectNode()
                                        .set("elements", arrayNode().add("/tmp/testpath"))))),
        new Case(CommitMeta.class)
            .obj(
                CommitMeta.builder()
                    .message("msg")
                    .hash(HASH)
                    .author("a1")
                    .author("a2")
                    .signedOffBy("s1")
                    .signedOffBy("s2")
                    .addParentCommitHashes("p1")
                    .addParentCommitHashes("p2")
                    .committer("c1")
                    .putAllProperties("p1", Arrays.asList("v1a", "v1b"))
                    .putProperties("p2", "v2")
                    .authorTime(Instant.ofEpochSecond(1))
                    .commitTime(Instant.ofEpochSecond(2))
                    .build())
            .view(Views.V2.class)
            .jsonNode(
                o -> {
                  o.put("hash", HASH)
                      .put("committer", "c1")
                      .set("authors", arrayNode().add("a1").add("a2"));
                  o.set("allSignedOffBy", arrayNode().add("s1").add("s2"));
                  o.put("message", "msg")
                      .put("commitTime", "1970-01-01T00:00:02Z")
                      .put("authorTime", "1970-01-01T00:00:01Z")
                      .set(
                          "allProperties",
                          ((ObjectNode) objectNode().set("p1", arrayNode().add("v1a").add("v1b")))
                              .set("p2", arrayNode().add("v2")));
                  o.set("parentCommitHashes", arrayNode().add("p1").add("p2"));
                }),
        new Case(LogResponse.class)
            .obj(
                LogResponse.builder()
                    .token(HASH)
                    .addLogEntries(
                        LogEntry.builder()
                            .commitMeta(
                                ImmutableCommitMeta.builder()
                                    .commitTime(now)
                                    .author("author@example.com")
                                    .committer("committer@example.com")
                                    .authorTime(now)
                                    .hash(HASH)
                                    .message("test commit")
                                    .putProperties("prop1", "val1")
                                    .signedOffBy("signer@example.com")
                                    .build())
                            .build())
                    .isHasMore(true)
                    .build())
            .jsonNode(
                o -> {
                  ObjectNode meta =
                      objectNode()
                          .put("hash", HASH)
                          .put("committer", "committer@example.com")
                          .put("author", "author@example.com")
                          .set("authors", arrayNode().add("author@example.com"));
                  meta.put("signedOffBy", "signer@example.com")
                      .set("allSignedOffBy", arrayNode().add("signer@example.com"));
                  meta.put("message", "test commit")
                      .put("commitTime", now.toString())
                      .put("authorTime", now.toString())
                      .set("properties", objectNode().put("prop1", "val1"));
                  meta.set("allProperties", objectNode().set("prop1", arrayNode().add("val1")));

                  o.put("token", HASH)
                      .put("hasMore", true)
                      .set(
                          "logEntries",
                          arrayNode()
                              .add(
                                  objectNode()
                                      .putNull("parentCommitHash")
                                      .putNull("operations")
                                      .set("commitMeta", meta)));
                }));
  }

  static List<Case> negativeCases() {
    return Arrays.asList(
        // Special chars in the branch name make it invalid
        new Case(Branch.class)
            .jsonNode(o -> o.put("type", "BRANCH").put("name", "$p@c!@L").put("hash", HASH)),

        // Invalid hash
        new Case(Branch.class)
            .jsonNode(
                o -> o.put("type", "BRANCH").put("name", "testBranch").put("hash", "invalidhash")),

        // No name
        new Case(Branch.class)
            .jsonNode(o -> o.put("type", "BRANCH").putNull("name").put("hash", HASH)),
        new Case(Tag.class)
            .jsonNode(o -> o.put("type", "TAG").put("name", "tagname").put("hash", "invalidhash")));
  }

  protected static class Case {

    Object obj;
    Class<?> serializationView;
    final Class<?> deserializeAs;
    ObjectNode deserializedJson = objectNode();
    boolean skipFinalCompare;

    public Case(Class<?> deserializeAs) {
      this.deserializeAs = deserializeAs;
    }

    public Case obj(Object obj) {
      this.obj = obj;
      return this;
    }

    public Case view(Class<?> serializationView) {
      this.serializationView = serializationView;
      return this;
    }

    public Case skipFinalCompare() {
      this.skipFinalCompare = true;
      return this;
    }

    public Case jsonNode(Consumer<ObjectNode> objNode) {
      objNode.accept(deserializedJson);
      return this;
    }

    @Override
    public String toString() {
      return deserializeAs.getName() + " : " + obj;
    }
  }
}

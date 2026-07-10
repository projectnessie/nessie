/*
 * Copyright (C) 2026 Dremio
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.projectnessie.model.GarbageCollectorConfig.ReferenceCutoffPolicy.referenceCutoffPolicy;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.projectnessie.error.ContentKeyErrorDetails;
import org.projectnessie.error.ErrorCode;
import org.projectnessie.error.GenericErrorDetails;
import org.projectnessie.error.ImmutableNessieError;
import org.projectnessie.error.NessieError;
import org.projectnessie.error.NessieErrorDetails;
import org.projectnessie.error.ReferenceConflicts;
import org.projectnessie.model.metadata.GenericContentMetadata;
import org.projectnessie.model.ser.Views;
import org.projectnessie.model.types.GenericContent;
import org.projectnessie.model.types.GenericRepositoryConfig;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.MapperFeature;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

class TestJackson3ModelCompatibility {

  private static final ObjectMapper MAPPER =
      JsonMapper.builder().enable(MapperFeature.DEFAULT_VIEW_INCLUSION).build();

  private static CommitMeta commitMeta() {
    return CommitMeta.builder()
        .hash("11223344")
        .committer("committer@example.com")
        .author("author@example.com")
        .signedOffBy("signer@example.com")
        .message("message")
        .commitTime(Instant.ofEpochSecond(2))
        .authorTime(Instant.ofEpochSecond(1))
        .putProperties("property", "value")
        .addParentCommitHashes("parent")
        .build();
  }

  @Test
  void contentRoundTripWithKnownType() throws Exception {
    Content content = IcebergTable.of("metadata.json", 1, 2, 3, 4, "content-id");

    String serialized = MAPPER.writeValueAsString(content);
    JsonNode serializedAsJson = MAPPER.readValue(serialized, JsonNode.class);
    assertThat(serializedAsJson.get("type").asString()).isEqualTo("ICEBERG_TABLE");
    assertThat(serializedAsJson.get("id").asString()).isEqualTo("content-id");

    Content deserialized = MAPPER.readValue(serialized, Content.class);
    assertThat(deserialized).isEqualTo(content);
  }

  @Test
  void contentRoundTripWithRemainingKnownTypes() throws Exception {
    for (Content content :
        new Content[] {
          IcebergView.of("view-id", "view-metadata.json", 1, 2),
          ImmutableDeltaLakeTable.builder()
              .id("delta-id")
              .addMetadataLocationHistory("metadata-1.json")
              .addCheckpointLocationHistory("checkpoint-1")
              .lastCheckpoint("checkpoint-1")
              .build(),
          Namespace.of(Map.of("owner", "nessie"), "a", "b"),
          UDF.udf("udf-id", "udf-metadata.json", "version", "signature")
        }) {
      String serialized = MAPPER.writeValueAsString(content);
      Content deserialized = MAPPER.readValue(serialized, Content.class);
      assertThat(deserialized).isEqualTo(content);
    }
  }

  @Test
  void contentRoundTripWithUnknownType() throws Exception {
    String json =
        "{\"type\":\"something-unknown\",\"id\":\"123\",\"a\":\"b\",\"nested\":{\"c\":\"d\"}}";

    Content content = MAPPER.readValue(json, Content.class);
    assertThat(content).isInstanceOf(GenericContent.class);
    assertThat(content.getType().name()).isEqualTo("something-unknown");
    assertThat(content.getId()).isEqualTo("123");

    String serialized = MAPPER.writeValueAsString(content);
    assertThat(MAPPER.readValue(serialized, JsonNode.class))
        .isEqualTo(MAPPER.readValue(json, JsonNode.class));
  }

  @Test
  void nessieConfigurationUsesJsonViews() throws Exception {
    NessieConfiguration configuration =
        ImmutableNessieConfiguration.builder()
            .defaultBranch("main")
            .minSupportedApiVersion(1)
            .maxSupportedApiVersion(2)
            .actualApiVersion(2)
            .specVersion("2.0.0")
            .repositoryCreationTimestamp(Instant.ofEpochSecond(10))
            .oldestPossibleCommitTimestamp(Instant.ofEpochSecond(9))
            .additionalProperties(Map.of())
            .build();

    JsonNode v1 =
        MAPPER.readValue(
            MAPPER.writerWithView(Views.V1.class).writeValueAsString(configuration),
            JsonNode.class);
    assertThat(v1.has("defaultBranch")).isTrue();
    assertThat(v1.has("minSupportedApiVersion")).isFalse();
    assertThat(v1.has("actualApiVersion")).isFalse();
    assertThat(v1.has("specVersion")).isFalse();

    JsonNode v2 =
        MAPPER.readValue(
            MAPPER.writerWithView(Views.V2.class).writeValueAsString(configuration),
            JsonNode.class);
    assertThat(v2.get("defaultBranch").asString()).isEqualTo("main");
    assertThat(v2.get("minSupportedApiVersion").intValue()).isEqualTo(1);
    assertThat(v2.get("maxSupportedApiVersion").intValue()).isEqualTo(2);
    assertThat(v2.get("actualApiVersion").intValue()).isEqualTo(2);
    assertThat(v2.get("specVersion").asString()).isEqualTo("2.0.0");
    assertThat(v2.get("repositoryCreationTimestamp").asString()).isEqualTo("1970-01-01T00:00:10Z");
    assertThat(v2.get("oldestPossibleCommitTimestamp").asString())
        .isEqualTo("1970-01-01T00:00:09Z");

    assertThat(
            MAPPER.readValue(MAPPER.writeValueAsString(configuration), NessieConfiguration.class))
        .isEqualTo(configuration);
  }

  @Test
  void repositoryConfigRoundTripWithKnownType() throws Exception {
    RepositoryConfig repositoryConfig =
        GarbageCollectorConfig.builder()
            .expectedFileCountPerContent(42)
            .defaultCutoffPolicy("3")
            .addPerRefCutoffPolicies(referenceCutoffPolicy("main", "P30D"))
            .newFilesGracePeriod(Duration.ofMinutes(5))
            .build();

    String serialized = MAPPER.writeValueAsString(repositoryConfig);
    JsonNode serializedAsJson = MAPPER.readValue(serialized, JsonNode.class);
    assertThat(serializedAsJson.get("type").asString()).isEqualTo("GARBAGE_COLLECTOR");
    assertThat(serializedAsJson.get("expectedFileCountPerContent").intValue()).isEqualTo(42);
    assertThat(serializedAsJson.get("newFilesGracePeriod").asString()).isEqualTo("PT5M");

    RepositoryConfig deserialized = MAPPER.readValue(serialized, RepositoryConfig.class);
    assertThat(deserialized).isEqualTo(repositoryConfig);
  }

  @Test
  void repositoryConfigRoundTripWithUnknownType() throws Exception {
    String json = "{\"type\":\"something-unknown\",\"a\":\"b\",\"nested\":{\"c\":\"d\"}}";

    RepositoryConfig repositoryConfig = MAPPER.readValue(json, RepositoryConfig.class);
    assertThat(repositoryConfig).isInstanceOf(GenericRepositoryConfig.class);
    assertThat(repositoryConfig.getType().name()).isEqualTo("something-unknown");

    String serialized = MAPPER.writeValueAsString(repositoryConfig);
    assertThat(MAPPER.readValue(serialized, JsonNode.class))
        .isEqualTo(MAPPER.readValue(json, JsonNode.class));
  }

  @Test
  void errorDetailsRoundTripWithKnownType() throws Exception {
    NessieErrorDetails errorDetails =
        ReferenceConflicts.referenceConflicts(
            Conflict.conflict(
                Conflict.ConflictType.UNEXPECTED_HASH, ContentKey.of("a", "b"), "message"));

    String serialized = MAPPER.writeValueAsString(errorDetails);
    JsonNode serializedAsJson = MAPPER.readValue(serialized, JsonNode.class);
    assertThat(serializedAsJson.get("type").asString()).isEqualTo("REFERENCE_CONFLICTS");

    NessieErrorDetails deserialized = MAPPER.readValue(serialized, NessieErrorDetails.class);
    assertThat(deserialized).isEqualTo(errorDetails);

    ContentKeyErrorDetails contentKeyErrorDetails =
        ContentKeyErrorDetails.contentKeyErrorDetails(ContentKey.of("a", "b"));
    assertThat(
            MAPPER.readValue(
                MAPPER.writeValueAsString(contentKeyErrorDetails), NessieErrorDetails.class))
        .isEqualTo(contentKeyErrorDetails);
  }

  @Test
  void errorDetailsRoundTripWithUnknownType() throws Exception {
    String json = "{\"type\":\"something-unknown\",\"a\":\"b\",\"nested\":{\"c\":\"d\"}}";

    NessieErrorDetails errorDetails = MAPPER.readValue(json, NessieErrorDetails.class);
    assertThat(errorDetails).isInstanceOf(GenericErrorDetails.class);
    assertThat(errorDetails.getType()).isEqualTo("something-unknown");

    String serialized = MAPPER.writeValueAsString(errorDetails);
    assertThat(MAPPER.readValue(serialized, JsonNode.class))
        .isEqualTo(MAPPER.readValue(json, JsonNode.class));
  }

  @Test
  void polymorphicFallbackRequiresTypeId() {
    assertThatThrownBy(() -> MAPPER.readValue("{\"id\":\"123\",\"a\":\"b\"}", Content.class))
        .isInstanceOf(tools.jackson.databind.exc.InvalidTypeIdException.class)
        .hasMessageContaining("missing type id property 'type'");

    assertThatThrownBy(() -> MAPPER.readValue("{\"a\":\"b\"}", RepositoryConfig.class))
        .isInstanceOf(tools.jackson.databind.exc.InvalidTypeIdException.class)
        .hasMessageContaining("missing type id property 'type'");

    assertThatThrownBy(() -> MAPPER.readValue("{\"a\":\"b\"}", NessieErrorDetails.class))
        .isInstanceOf(tools.jackson.databind.exc.InvalidTypeIdException.class)
        .hasMessageContaining("missing type id property 'type'");
  }

  @Test
  void nessieErrorRoundTrip() throws Exception {
    NessieErrorDetails details =
        ReferenceConflicts.referenceConflicts(
            Conflict.conflict(
                Conflict.ConflictType.UNEXPECTED_HASH, ContentKey.of("a", "b"), "message"));
    NessieError error =
        ImmutableNessieError.builder()
            .message("message")
            .errorCode(ErrorCode.REFERENCE_CONFLICT)
            .status(409)
            .reason("Conflict")
            .serverStackTrace("server-stack")
            .clientProcessingError("client-processing")
            .errorDetails(details)
            .build();

    String serialized = MAPPER.writeValueAsString(error);
    JsonNode serializedAsJson = MAPPER.readValue(serialized, JsonNode.class);
    assertThat(serializedAsJson.get("errorCode").asString()).isEqualTo("REFERENCE_CONFLICT");
    assertThat(serializedAsJson.get("errorDetails").get("type").asString())
        .isEqualTo("REFERENCE_CONFLICTS");
    assertThat(serializedAsJson.has("clientProcessingError")).isFalse();

    NessieError deserialized = MAPPER.readValue(serialized, NessieError.class);
    NessieError expected =
        ImmutableNessieError.builder().from(error).clientProcessingError(null).build();
    assertThat(deserialized).isEqualTo(expected);
  }

  @Test
  void nessieErrorUnknownErrorCodeFallsBackToUnknown() throws Exception {
    String json =
        """
        {
          "status": 500,
          "reason": "Internal Server Error",
          "message": "message",
          "errorCode": "SOMETHING_NEW"
        }
        """;

    NessieError error = MAPPER.readValue(json, NessieError.class);
    assertThat(error.getErrorCode()).isEqualTo(ErrorCode.UNKNOWN);
  }

  @Test
  void referenceRoundTripWithKnownTypes() throws Exception {
    for (Reference reference :
        new Reference[] {Branch.of("main", "12345678"), Tag.of("tag", "87654321")}) {
      String serialized = MAPPER.writeValueAsString(reference);
      Reference deserialized = MAPPER.readValue(serialized, Reference.class);
      assertThat(deserialized).isEqualTo(reference);
    }

    Detached detached = Detached.of("12345678");
    String serialized = MAPPER.writeValueAsString(detached);
    JsonNode serializedAsJson = MAPPER.readValue(serialized, JsonNode.class);
    assertThat(serializedAsJson.get("type").asString()).isEqualTo("DETACHED");
    assertThat(MAPPER.readValue(serialized, Reference.class)).isEqualTo(detached);
  }

  @Test
  void referencesResponseRoundTrip() throws Exception {
    ReferencesResponse references =
        ReferencesResponse.builder()
            .addReferences(Branch.of("main", "12345678"))
            .addReferences(Tag.of("tag", "87654321"))
            .token("next-page")
            .isHasMore(true)
            .build();

    String serialized = MAPPER.writeValueAsString(references);
    JsonNode serializedAsJson = MAPPER.readValue(serialized, JsonNode.class);
    assertThat(serializedAsJson.get("references").get(0).get("type").asString())
        .isEqualTo("BRANCH");
    assertThat(serializedAsJson.get("references").get(1).get("type").asString()).isEqualTo("TAG");

    ReferencesResponse deserialized = MAPPER.readValue(serialized, ReferencesResponse.class);
    assertThat(deserialized).isEqualTo(references);
  }

  @Test
  void referenceMetadataRoundTripWithCommitMeta() throws Exception {
    ReferenceMetadata metadata =
        ImmutableReferenceMetadata.builder()
            .numCommitsAhead(1)
            .numCommitsBehind(2)
            .commitMetaOfHEAD(commitMeta())
            .commonAncestorHash("ancestor")
            .numTotalCommits(3L)
            .build();
    Reference reference = Branch.of("main", "12345678", metadata);

    String serialized = MAPPER.writeValueAsString(reference);
    Reference deserialized = MAPPER.readValue(serialized, Reference.class);
    assertThat(deserialized).isEqualTo(reference);

    JsonNode serializedAsJson = MAPPER.readValue(serialized, JsonNode.class);
    assertThat(
            serializedAsJson.get("metadata").get("commitMetaOfHEAD").get("commitTime").asString())
        .isEqualTo("1970-01-01T00:00:02Z");
  }

  @Test
  void commitMetaAcceptsV1Shape() throws Exception {
    String json =
        """
        {
          "hash": "11223344",
          "committer": "committer@example.com",
          "author": "author@example.com",
          "signedOffBy": "signer@example.com",
          "message": "message",
          "commitTime": "1970-01-01T00:00:02Z",
          "authorTime": "1970-01-01T00:00:01Z",
          "properties": {
            "property": "value"
          },
          "parentCommitHashes": ["parent"]
        }
        """;

    CommitMeta deserialized = MAPPER.readValue(json, CommitMeta.class);
    assertThat(deserialized).isEqualTo(commitMeta());
  }

  @Test
  void operationsRoundTrip() throws Exception {
    Operations operations =
        ImmutableOperations.builder()
            .commitMeta(commitMeta())
            .addOperations(
                Operation.Put.of(
                    ContentKey.of("table"), IcebergTable.of("metadata.json", 1, 2, 3, 4)))
            .addOperations(Operation.Delete.of(ContentKey.of("deleted")))
            .addOperations(Operation.Unchanged.of(ContentKey.of("unchanged")))
            .build();

    String serialized = MAPPER.writeValueAsString(operations);
    JsonNode serializedAsJson = MAPPER.readValue(serialized, JsonNode.class);
    assertThat(serializedAsJson.get("operations").get(0).get("type").asString()).isEqualTo("PUT");
    assertThat(serializedAsJson.get("operations").get(1).get("type").asString())
        .isEqualTo("DELETE");
    assertThat(serializedAsJson.get("operations").get(2).get("type").asString())
        .isEqualTo("UNCHANGED");

    Operations deserialized = MAPPER.readValue(serialized, Operations.class);
    assertThat(deserialized).isEqualTo(operations);
  }

  @Test
  void commitAndLogResponsesRoundTrip() throws Exception {
    CommitResponse commitResponse =
        CommitResponse.builder()
            .targetBranch(Branch.of("main", "12345678"))
            .addAddedContents(
                CommitResponse.AddedContent.addedContent(ContentKey.of("table"), "id"))
            .build();
    assertThat(MAPPER.readValue(MAPPER.writeValueAsString(commitResponse), CommitResponse.class))
        .isEqualTo(commitResponse);

    LogResponse logResponse =
        LogResponse.builder()
            .addLogEntries(
                LogResponse.LogEntry.builder()
                    .commitMeta(commitMeta())
                    .operations(
                        List.of(
                            Operation.Put.of(
                                ContentKey.of("table"),
                                IcebergTable.of("metadata.json", 1, 2, 3, 4))))
                    .build())
            .isHasMore(true)
            .token("next-page")
            .build();
    assertThat(MAPPER.readValue(MAPPER.writeValueAsString(logResponse), LogResponse.class))
        .isEqualTo(logResponse);
  }

  @Test
  void mergeResponseRoundTrip() throws Exception {
    MergeResponse mergeResponse =
        ImmutableMergeResponse.builder()
            .wasApplied(true)
            .wasSuccessful(false)
            .targetBranch("main")
            .effectiveTargetHash("12345678")
            .resultantTargetHash("87654321")
            .expectedHash("11111111")
            .addDetails(
                ImmutableContentKeyDetails.builder()
                    .key(ContentKey.of("table"))
                    .mergeBehavior(MergeBehavior.NORMAL)
                    .conflict(
                        Conflict.conflict(
                            Conflict.ConflictType.UNEXPECTED_HASH,
                            ContentKey.of("table"),
                            "message"))
                    .build())
            .build();

    assertThat(MAPPER.readValue(MAPPER.writeValueAsString(mergeResponse), MergeResponse.class))
        .isEqualTo(mergeResponse);
  }

  @Test
  void contentAndDiffResponsesRoundTrip() throws Exception {
    Content content = IcebergTable.of("metadata.json", 1, 2, 3, 4, "content-id");
    Reference reference = Branch.of("main", "12345678");
    Documentation documentation = Documentation.of("text/plain", "documentation");

    ContentResponse contentResponse = ContentResponse.of(content, reference, documentation);
    assertThat(MAPPER.readValue(MAPPER.writeValueAsString(contentResponse), ContentResponse.class))
        .isEqualTo(contentResponse);

    GetMultipleContentsRequest request =
        GetMultipleContentsRequest.of(ContentKey.of("table"), ContentKey.of("view"));
    assertThat(
            MAPPER.readValue(MAPPER.writeValueAsString(request), GetMultipleContentsRequest.class))
        .isEqualTo(request);

    GetMultipleContentsResponse response =
        GetMultipleContentsResponse.of(
            List.of(GetMultipleContentsResponse.ContentWithKey.of(ContentKey.of("table"), content)),
            reference);
    assertThat(
            MAPPER.readValue(
                MAPPER.writeValueAsString(response), GetMultipleContentsResponse.class))
        .isEqualTo(response);

    DiffResponse diffResponse =
        DiffResponse.builder()
            .addDiffs(DiffResponse.DiffEntry.diffEntry(ContentKey.of("table"), null, content))
            .effectiveFromReference(reference)
            .effectiveToReference(Tag.of("tag", "87654321"))
            .build();
    assertThat(MAPPER.readValue(MAPPER.writeValueAsString(diffResponse), DiffResponse.class))
        .isEqualTo(diffResponse);
  }

  @Test
  void singleReferenceResponseRoundTrip() throws Exception {
    SingleReferenceResponse response =
        SingleReferenceResponse.builder().reference(Branch.of("main", "12345678")).build();

    assertThat(MAPPER.readValue(MAPPER.writeValueAsString(response), SingleReferenceResponse.class))
        .isEqualTo(response);
  }

  @Test
  void namespaceResponseRoundTrip() throws Exception {
    GetNamespacesResponse response =
        GetNamespacesResponse.builder()
            .addNamespaces(Namespace.of("a", "b"))
            .addNamespaces(Namespace.of(Map.of("owner", "nessie"), "c"))
            .effectiveReference(Branch.of("main", "12345678"))
            .build();

    String serialized = MAPPER.writeValueAsString(response);
    JsonNode serializedAsJson = MAPPER.readValue(serialized, JsonNode.class);
    assertThat(serializedAsJson.get("namespaces").get(0).get("type").asString())
        .isEqualTo("NAMESPACE");

    assertThat(MAPPER.readValue(serialized, GetNamespacesResponse.class)).isEqualTo(response);
  }

  @Test
  void identifiedContentKeyRoundTrip() throws Exception {
    IdentifiedContentKey identifiedContentKey =
        IdentifiedContentKey.identifiedContentKeyFromContent(
            ContentKey.of("a", "table"),
            IcebergTable.of("metadata.json", 1, 2, 3, 4, "content-id"),
            path -> "namespace-id");

    assertThat(
            MAPPER.readValue(
                MAPPER.writeValueAsString(identifiedContentKey), IdentifiedContentKey.class))
        .isEqualTo(identifiedContentKey);
  }

  @Test
  void repositoryConfigWrappersRoundTrip() throws Exception {
    RepositoryConfig repositoryConfig =
        GarbageCollectorConfig.builder()
            .expectedFileCountPerContent(42)
            .defaultCutoffPolicy("3")
            .build();

    RepositoryConfigResponse response =
        ImmutableRepositoryConfigResponse.builder().addConfigs(repositoryConfig).build();
    assertThat(
            MAPPER.readValue(MAPPER.writeValueAsString(response), RepositoryConfigResponse.class))
        .isEqualTo(response);

    UpdateRepositoryConfigRequest request =
        ImmutableUpdateRepositoryConfigRequest.builder().config(repositoryConfig).build();
    assertThat(
            MAPPER.readValue(
                MAPPER.writeValueAsString(request), UpdateRepositoryConfigRequest.class))
        .isEqualTo(request);

    UpdateRepositoryConfigResponse updateResponse =
        ImmutableUpdateRepositoryConfigResponse.builder().previous(repositoryConfig).build();
    assertThat(
            MAPPER.readValue(
                MAPPER.writeValueAsString(updateResponse), UpdateRepositoryConfigResponse.class))
        .isEqualTo(updateResponse);
  }

  @Test
  @SuppressWarnings("deprecation")
  void referenceHistoryAndRefLogRoundTrip() throws Exception {
    ReferenceHistoryState state =
        ReferenceHistoryState.referenceHistoryElement(
            "12345678", CommitConsistency.COMMIT_CONSISTENT, commitMeta());
    ReferenceHistoryResponse history =
        ReferenceHistoryResponse.builder()
            .reference(Branch.of("main", "12345678"))
            .current(state)
            .addPrevious(
                ReferenceHistoryState.referenceHistoryElement(
                    "87654321", CommitConsistency.NOT_CHECKED, null))
            .commitLogConsistency(CommitConsistency.COMMIT_CONSISTENT)
            .build();

    assertThat(MAPPER.readValue(MAPPER.writeValueAsString(history), ReferenceHistoryResponse.class))
        .isEqualTo(history);

    RefLogResponse refLog =
        ImmutableRefLogResponse.builder()
            .addLogEntries(
                RefLogResponse.RefLogResponseEntry.builder()
                    .refLogId("reflog")
                    .refName("main")
                    .refType(RefLogResponse.RefLogResponseEntry.BRANCH)
                    .commitHash("12345678")
                    .parentRefLogId("parent")
                    .operationTime(42L)
                    .operation(RefLogResponse.RefLogResponseEntry.COMMIT)
                    .addSourceHashes("source")
                    .build())
            .isHasMore(true)
            .token("next-page")
            .build();

    assertThat(MAPPER.readValue(MAPPER.writeValueAsString(refLog), RefLogResponse.class))
        .isEqualTo(refLog);
  }

  @Test
  void mergeKeyBehaviorRoundTrip() throws Exception {
    MergeKeyBehavior behavior =
        MergeKeyBehavior.builder()
            .key(ContentKey.of("table"))
            .mergeBehavior(MergeBehavior.FORCE)
            .expectedTargetContent(IcebergTable.of("metadata.json", 1, 2, 3, 4, "expected"))
            .resolvedContent(IcebergTable.of("metadata-new.json", 5, 6, 7, 8, "resolved"))
            .expectedTargetDocumentation(Documentation.of("text/plain", "old"))
            .resolvedDocumentation(Documentation.of("text/plain", "new"))
            .addMetadata(
                GenericContentMetadata.genericContentMetadata("custom", Map.of("key", "value")))
            .build();

    assertThat(MAPPER.readValue(MAPPER.writeValueAsString(behavior), MergeKeyBehavior.class))
        .isEqualTo(behavior);
  }

  @Test
  @SuppressWarnings("deprecation")
  void v1ParamsRoundTrip() throws Exception {
    String hash = "3e1cfa82";
    MergeKeyBehavior keyBehavior =
        MergeKeyBehavior.of(ContentKey.of("merge", "me"), MergeBehavior.NORMAL);

    org.projectnessie.api.v1.params.Merge merge =
        org.projectnessie.api.v1.params.ImmutableMerge.builder()
            .fromRefName("main")
            .fromHash(hash)
            .keepIndividualCommits(true)
            .defaultKeyMergeMode(MergeBehavior.FORCE)
            .isDryRun(false)
            .isFetchAdditionalInfo(true)
            .isReturnConflictAsResult(true)
            .addKeyMergeModes(keyBehavior)
            .build();
    assertThat(
            MAPPER.readValue(
                MAPPER.writeValueAsString(merge), org.projectnessie.api.v1.params.Merge.class))
        .isEqualTo(merge);

    org.projectnessie.api.v1.params.Transplant transplant =
        org.projectnessie.api.v1.params.ImmutableTransplant.builder()
            .fromRefName("main")
            .addHashesToTransplant(hash)
            .keepIndividualCommits(true)
            .defaultKeyMergeMode(MergeBehavior.DROP)
            .isDryRun(true)
            .addKeyMergeModes(keyBehavior)
            .build();
    assertThat(
            MAPPER.readValue(
                MAPPER.writeValueAsString(transplant),
                org.projectnessie.api.v1.params.Transplant.class))
        .isEqualTo(transplant);

    org.projectnessie.api.v1.params.NamespaceUpdate namespaceUpdate =
        org.projectnessie.api.v1.params.ImmutableNamespaceUpdate.builder()
            .propertyUpdates(Map.of("owner", "nessie"))
            .propertyRemovals(Set.of("old-owner"))
            .build();
    assertThat(
            MAPPER.readValue(
                MAPPER.writeValueAsString(namespaceUpdate),
                org.projectnessie.api.v1.params.NamespaceUpdate.class))
        .isEqualTo(namespaceUpdate);
  }

  @Test
  @SuppressWarnings("deprecation")
  void v2ParamsRoundTrip() throws Exception {
    String hash = "3e1cfa82";
    MergeKeyBehavior keyBehavior =
        MergeKeyBehavior.of(ContentKey.of("merge", "me"), MergeBehavior.NORMAL);

    org.projectnessie.api.v2.params.Merge merge =
        org.projectnessie.api.v2.params.ImmutableMerge.builder()
            .fromRefName("main")
            .fromHash(hash)
            .message("deprecated-message")
            .commitMeta(commitMeta())
            .defaultKeyMergeMode(MergeBehavior.FORCE)
            .isDryRun(false)
            .isFetchAdditionalInfo(true)
            .isReturnConflictAsResult(true)
            .addKeyMergeModes(keyBehavior)
            .build();
    assertThat(
            MAPPER.readValue(
                MAPPER.writeValueAsString(merge), org.projectnessie.api.v2.params.Merge.class))
        .isEqualTo(merge);

    org.projectnessie.api.v2.params.Transplant transplant =
        org.projectnessie.api.v2.params.ImmutableTransplant.builder()
            .fromRefName("main")
            .message("message")
            .addHashesToTransplant(hash)
            .addHashesToTransplant("cafebabe~1")
            .defaultKeyMergeMode(MergeBehavior.DROP)
            .isDryRun(true)
            .addKeyMergeModes(keyBehavior)
            .build();
    assertThat(
            MAPPER.readValue(
                MAPPER.writeValueAsString(transplant),
                org.projectnessie.api.v2.params.Transplant.class))
        .isEqualTo(transplant);
  }
}

/*
 * Copyright (C) 2023 Dremio
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
package org.projectnessie.jaxrs.tests;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Maps.immutableEntry;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;
import static org.assertj.core.api.InstanceOfAssertFactories.list;
import static org.assertj.core.api.InstanceOfAssertFactories.type;
import static org.projectnessie.model.CommitMeta.fromMessage;
import static org.projectnessie.model.FetchOption.ALL;
import static org.projectnessie.model.Namespace.Empty.EMPTY_NAMESPACE;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.hash.Hashing;
import jakarta.validation.constraints.NotNull;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.assertj.core.api.AbstractThrowableAssert;
import org.assertj.core.api.ListAssert;
import org.assertj.core.api.SoftAssertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.projectnessie.client.api.AssignReferenceBuilder;
import org.projectnessie.client.api.CommitMultipleOperationsBuilder;
import org.projectnessie.client.api.CreateNamespaceResult;
import org.projectnessie.client.api.DeleteNamespaceResult;
import org.projectnessie.client.api.DeleteReferenceBuilder;
import org.projectnessie.client.api.NessieApiV1;
import org.projectnessie.client.api.NessieApiV2;
import org.projectnessie.client.api.UpdateNamespaceResult;
import org.projectnessie.client.ext.NessieApiVersion;
import org.projectnessie.client.ext.NessieApiVersions;
import org.projectnessie.client.ext.NessieClientFactory;
import org.projectnessie.error.BaseNessieClientServerException;
import org.projectnessie.error.ContentKeyErrorDetails;
import org.projectnessie.error.ErrorCode;
import org.projectnessie.error.NessieBadRequestException;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieContentNotFoundException;
import org.projectnessie.error.NessieNamespaceNotEmptyException;
import org.projectnessie.error.NessieNamespaceNotFoundException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.error.NessieReferenceConflictException;
import org.projectnessie.error.ReferenceConflicts;
import org.projectnessie.model.Branch;
import org.projectnessie.model.CommitConsistency;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.CommitResponse;
import org.projectnessie.model.CommitResponse.AddedContent;
import org.projectnessie.model.Conflict;
import org.projectnessie.model.Conflict.ConflictType;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.ContentResponse;
import org.projectnessie.model.DiffResponse;
import org.projectnessie.model.DiffResponse.DiffEntry;
import org.projectnessie.model.EntriesResponse;
import org.projectnessie.model.EntriesResponse.Entry;
import org.projectnessie.model.GarbageCollectorConfig;
import org.projectnessie.model.GetMultipleContentsResponse;
import org.projectnessie.model.GetNamespacesResponse;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.IcebergView;
import org.projectnessie.model.ImmutableIcebergTable;
import org.projectnessie.model.ImmutableReferenceMetadata;
import org.projectnessie.model.LogResponse;
import org.projectnessie.model.LogResponse.LogEntry;
import org.projectnessie.model.MergeResponse.ContentKeyConflict;
import org.projectnessie.model.MergeResponse.ContentKeyDetails;
import org.projectnessie.model.Namespace;
import org.projectnessie.model.NessieConfiguration;
import org.projectnessie.model.Operation;
import org.projectnessie.model.Operation.Delete;
import org.projectnessie.model.Operation.Put;
import org.projectnessie.model.Reference;
import org.projectnessie.model.Reference.ReferenceType;
import org.projectnessie.model.ReferenceHistoryResponse;
import org.projectnessie.model.ReferenceHistoryState;
import org.projectnessie.model.ReferencesResponse;
import org.projectnessie.model.RepositoryConfig;
import org.projectnessie.model.Tag;
import org.projectnessie.model.UDF;
import org.projectnessie.model.Validation;
import org.projectnessie.model.types.GenericRepositoryConfig;
import org.projectnessie.model.types.ImmutableGenericRepositoryConfig;

/** Nessie-API tests. */
@org.junit.jupiter.api.Tag("nessie-multi-env")
@NessieApiVersions // all versions
public abstract class BaseTestNessieApi {

  public static final String EMPTY = Hashing.sha256().hashString("empty", UTF_8).toString();

  private NessieApiV1 api;
  private NessieApiVersion apiVersion;

  // Cannot use @ExtendWith(SoftAssertionsExtension.class) + @InjectSoftAssertions here, because
  // of Quarkus class loading issues. See https://github.com/quarkusio/quarkus/issues/19814
  protected final SoftAssertions soft = new SoftAssertions();

  static {
    // Note: REST tests validate some locale-specific error messages, but expect on the messages to
    // be in ENGLISH. However, the JRE's startup classes (in particular class loaders) may cause the
    // default Locale to be initialized before Maven is able to override the user.language system
    // property. Therefore, we explicitly set the default Locale to ENGLISH here to match tests'
    // expectations.
    Locale.setDefault(Locale.ENGLISH);
  }

  @SuppressWarnings("JUnitMalformedDeclaration")
  @BeforeEach
  void initApi(NessieClientFactory clientFactory) {
    this.api = clientFactory.make();
    this.apiVersion = clientFactory.apiVersion();
  }

  @NotNull
  public NessieApiV1 api() {
    return api;
  }

  public NessieApiV2 apiV2() {
    checkState(api instanceof NessieApiV2, "Not using API v2");
    return (NessieApiV2) api;
  }

  public boolean isV2() {
    return NessieApiVersion.V2 == apiVersion;
  }

  @AfterEach
  public void tearDown() throws Exception {
    try {
      // Cannot use @ExtendWith(SoftAssertionsExtension.class) + @InjectSoftAssertions here, because
      // of Quarkus class loading issues. See https://github.com/quarkusio/quarkus/issues/19814
      soft.assertAll();
    } finally {
      Branch defaultBranch = api.getDefaultBranch();
      api()
          .assignBranch()
          .branch(defaultBranch)
          .assignTo(Branch.of(defaultBranch.getName(), EMPTY))
          .assign();
      api.getAllReferences().stream()
          .forEach(
              ref -> {
                try {
                  if (ref instanceof Branch && !ref.getName().equals(defaultBranch.getName())) {
                    api.deleteBranch().branch((Branch) ref).delete();
                  } else if (ref instanceof Tag) {
                    api.deleteTag().tag((Tag) ref).delete();
                  }
                } catch (NessieConflictException | NessieNotFoundException e) {
                  throw new RuntimeException(e);
                }
              });
      api.close();
    }
  }

  @SuppressWarnings("unchecked")
  protected <R extends Reference> R createReference(R reference, String sourceRefName)
      throws NessieConflictException, NessieNotFoundException {
    return (R) api().createReference().sourceRefName(sourceRefName).reference(reference).create();
  }

  protected CommitMultipleOperationsBuilder prepCommit(
      Branch branch, String msg, Operation... operations) {
    return api()
        .commitMultipleOperations()
        .branch(branch)
        .commitMeta(fromMessage(msg))
        .operations(asList(operations));
  }

  protected Put dummyPut(String... elements) {
    return Put.of(ContentKey.of(elements), dummyTable());
  }

  private static IcebergTable dummyTable() {
    return IcebergTable.of("foo", 1, 2, 3, 4);
  }

  @Test
  public void config() throws NessieNotFoundException {
    NessieConfiguration config = api().getConfig();
    soft.assertThat(config)
        .extracting(
            NessieConfiguration::getDefaultBranch, NessieConfiguration::getMaxSupportedApiVersion)
        .containsExactly("main", 2);

    if (isV2()) {
      soft.assertThat(config.getNoAncestorHash()).isNotNull();
    }

    soft.assertThat(api().getDefaultBranch())
        .extracting(Branch::getName, Branch::getHash)
        .containsExactly(config.getDefaultBranch(), EMPTY);
  }

  @Test
  @NessieApiVersions(versions = NessieApiVersion.V2)
  public void specVersion() {
    NessieConfiguration config = api().getConfig();
    soft.assertThat(config.getSpecVersion()).isNotEmpty();
  }

  @Test
  public void references() throws Exception {
    Branch main = api().getDefaultBranch();
    soft.assertThat(api().getAllReferences().get().getReferences()).containsExactly(main);

    Branch main1 =
        prepCommit(
                main,
                "commit",
                Put.of(ContentKey.of("key"), Namespace.of("key")),
                dummyPut("key", "foo"))
            .commit();

    CommitMeta commitMetaMain =
        api().getCommitLog().reference(main1).get().getLogEntries().get(0).getCommitMeta();

    Tag tag = createReference(Tag.of("tag1", main1.getHash()), main.getName());

    Branch branch = createReference(Branch.of("branch1", main1.getHash()), main.getName());

    Branch branch1 = prepCommit(branch, "branch", dummyPut("key", "bar")).commit();

    CommitMeta commitMetaBranch =
        api().getCommitLog().reference(branch1).get().getLogEntries().get(0).getCommitMeta();

    // Get references

    soft.assertThat(api().getAllReferences().get().getReferences())
        .containsExactlyInAnyOrder(main1, tag, branch1);
    soft.assertThat(api().getReference().refName(main.getName()).get()).isEqualTo(main1);
    soft.assertThat(api().getReference().refName(tag.getName()).get()).isEqualTo(tag);
    soft.assertThat(api().getReference().refName(branch.getName()).get()).isEqualTo(branch1);

    // Get references / FULL

    Branch mainFull =
        Branch.of(
            main.getName(),
            main1.getHash(),
            ImmutableReferenceMetadata.builder()
                .numTotalCommits(1L)
                .commitMetaOfHEAD(commitMetaMain)
                .build());
    Branch branchFull =
        Branch.of(
            branch.getName(),
            branch1.getHash(),
            ImmutableReferenceMetadata.builder()
                .numTotalCommits(2L)
                .numCommitsAhead(1)
                .numCommitsBehind(0)
                .commonAncestorHash(main1.getHash())
                .commitMetaOfHEAD(commitMetaBranch)
                .build());
    Tag tagFull =
        Tag.of(
            tag.getName(),
            tag.getHash(),
            ImmutableReferenceMetadata.builder()
                .numTotalCommits(1L)
                .commitMetaOfHEAD(commitMetaMain)
                .build());
    soft.assertThat(api().getAllReferences().fetch(ALL).get().getReferences())
        .containsExactlyInAnyOrder(mainFull, branchFull, tagFull);
    soft.assertThat(api().getReference().refName(main.getName()).fetch(ALL).get())
        .isEqualTo(mainFull);
    soft.assertThat(api().getReference().refName(tag.getName()).fetch(ALL).get())
        .isEqualTo(tagFull);
    soft.assertThat(api().getReference().refName(branch.getName()).fetch(ALL).get())
        .isEqualTo(branchFull);

    // Assign

    if (isV2()) {
      tag = api.assignTag().tag(tag).assignTo(main).assignAndGet();
      soft.assertThat(tag).isEqualTo(Tag.of(tag.getName(), main.getHash()));
    } else {
      api.assignTag().tag(tag).assignTo(main).assign();
      tag = Tag.of(tag.getName(), main.getHash());
    }

    AbstractThrowableAssert<?, ? extends Throwable> assignConflict =
        isV2()
            ? soft.assertThatThrownBy(
                () -> api.assignBranch().branch(branch).assignTo(main).assignAndGet())
            : soft.assertThatThrownBy(
                () -> api.assignBranch().branch(branch).assignTo(main).assign());
    assignConflict
        .isInstanceOf(NessieReferenceConflictException.class)
        .asInstanceOf(type(NessieReferenceConflictException.class))
        .extracting(NessieReferenceConflictException::getErrorDetails)
        .extracting(ReferenceConflicts::conflicts, list(Conflict.class))
        .extracting(Conflict::conflictType)
        .containsExactly(ConflictType.UNEXPECTED_HASH);

    Branch branchAssigned;
    if (isV2()) {
      branchAssigned = api.assignBranch().branch(branch1).assignTo(main).assignAndGet();
      soft.assertThat(branchAssigned).isEqualTo(Branch.of(branch.getName(), main.getHash()));
    } else {
      api.assignBranch().branch(branch1).assignTo(main).assign();
      branchAssigned = Branch.of(branch.getName(), main.getHash());
    }

    // check

    soft.assertThat(api().getAllReferences().get().getReferences())
        .containsExactlyInAnyOrder(main1, tag, branchAssigned);
    soft.assertThat(api().getReference().refName(main.getName()).get()).isEqualTo(main1);
    soft.assertThat(api().getReference().refName(tag.getName()).get()).isEqualTo(tag);
    soft.assertThat(api().getReference().refName(branch.getName()).get()).isEqualTo(branchAssigned);

    // Delete

    if (isV2()) {
      Tag deleted = api().deleteTag().tag(tag).getAndDelete();
      soft.assertThat(deleted).isEqualTo(tag);
    } else {
      api().deleteTag().tag(tag).delete();
    }

    AbstractThrowableAssert<?, ? extends Throwable> deleteConflict =
        isV2()
            ? soft.assertThatThrownBy(() -> api().deleteBranch().branch(branch).getAndDelete())
            : soft.assertThatThrownBy(() -> api().deleteBranch().branch(branch).delete());
    deleteConflict
        .isInstanceOf(NessieReferenceConflictException.class)
        .asInstanceOf(type(NessieReferenceConflictException.class))
        .extracting(NessieReferenceConflictException::getErrorDetails)
        .extracting(ReferenceConflicts::conflicts, list(Conflict.class))
        .extracting(Conflict::conflictType)
        .containsExactly(ConflictType.UNEXPECTED_HASH);

    if (isV2()) {
      Branch deleted = api().deleteBranch().branch(branchAssigned).getAndDelete();
      soft.assertThat(deleted).isEqualTo(branchAssigned);
    } else {
      api().deleteBranch().branch(branchAssigned).delete();
    }

    soft.assertThat(api().getAllReferences().get().getReferences())
        .containsExactlyInAnyOrder(main1);
    soft.assertThat(api().getReference().refName(main.getName()).get()).isEqualTo(main1);

    // Create / null hash

    soft.assertThatThrownBy(() -> createReference(Tag.of("tag2", null), main.getName()))
        .isInstanceOf(NessieBadRequestException.class);

    soft.assertThatThrownBy(() -> createReference(Branch.of("branch2", null), main.getName()))
        .isInstanceOf(NessieBadRequestException.class);

    // not exist

    String refName = "does-not-exist";
    soft.assertThatThrownBy(() -> api().getReference().refName(refName).get())
        .isInstanceOf(NessieNotFoundException.class);
    soft.assertThatThrownBy(
            () ->
                api()
                    .assignBranch()
                    .branch(Branch.of(refName, main.getHash()))
                    .assignTo(main)
                    .assign())
        .isInstanceOf(NessieNotFoundException.class);
    soft.assertThatThrownBy(
            () -> api().assignTag().tag(Tag.of(refName, main.getHash())).assignTo(main).assign())
        .isInstanceOf(NessieNotFoundException.class);
    soft.assertThatThrownBy(
            () -> api().deleteBranch().branch(Branch.of(refName, main.getHash())).delete())
        .isInstanceOf(NessieNotFoundException.class);
    soft.assertThatThrownBy(() -> api().deleteTag().tag(Tag.of(refName, main.getHash())).delete())
        .isInstanceOf(NessieNotFoundException.class);
    if (isV2()) {
      soft.assertThatThrownBy(
              () ->
                  api()
                      .assignBranch()
                      .branch(Branch.of(refName, main.getHash()))
                      .assignTo(main)
                      .assignAndGet())
          .isInstanceOf(NessieNotFoundException.class);
      soft.assertThatThrownBy(
              () ->
                  api()
                      .assignTag()
                      .tag(Tag.of(refName, main.getHash()))
                      .assignTo(main)
                      .assignAndGet())
          .isInstanceOf(NessieNotFoundException.class);
      soft.assertThatThrownBy(
              () -> api().deleteBranch().branch(Branch.of(refName, main.getHash())).getAndDelete())
          .isInstanceOf(NessieNotFoundException.class);
      soft.assertThatThrownBy(
              () -> api().deleteTag().tag(Tag.of(refName, main.getHash())).getAndDelete())
          .isInstanceOf(NessieNotFoundException.class);
    }
  }

  @ParameterizedTest
  @EnumSource(ReferenceType.class)
  @NessieApiVersions(versions = NessieApiVersion.V2)
  public void referencesUntyped(ReferenceType referenceType) throws Exception {
    Branch main = api().getDefaultBranch();
    soft.assertThat(api().getAllReferences().get().getReferences()).containsExactly(main);

    Branch main1 =
        prepCommit(
                main,
                "commit",
                Put.of(ContentKey.of("key"), Namespace.of("key")),
                dummyPut("key", "foo"))
            .commit();

    Reference ref0;
    ReferenceType anotherType;
    switch (referenceType) {
      case TAG:
        ref0 = Tag.of("tag1", main.getHash());
        anotherType = ReferenceType.BRANCH;
        break;
      case BRANCH:
        ref0 = Branch.of("branch1", main.getHash());
        anotherType = ReferenceType.TAG;
        break;
      default:
        throw new IllegalStateException("Unsupported ref type: " + referenceType);
    }

    Reference ref = createReference(ref0, main.getName());

    @SuppressWarnings("resource")
    NessieApiV2 api = apiV2();

    soft.assertThatThrownBy(
            () -> api.assignReference().refName(ref0.getName()).assignTo(main1).assign())
        .isInstanceOf(NessieBadRequestException.class)
        .hasMessageContaining("Expected hash must be provided");

    soft.assertThatThrownBy(
            () ->
                api.assignReference()
                    .refName(ref0.getName())
                    .hash(ref0.getHash())
                    .refType(anotherType)
                    .assignTo(main1)
                    .assign())
        .isInstanceOf(NessieBadRequestException.class)
        .hasMessageMatching(".*Expected reference type .+ does not match existing reference.*");

    ref = api.assignReference().reference(ref).assignTo(main1).assignAndGet();
    soft.assertThat(ref)
        .extracting(Reference::getName, Reference::getHash)
        .containsExactly(ref0.getName(), main1.getHash());

    ref =
        api.assignReference()
            .refName(ref.getName())
            .hash(ref.getHash())
            .assignTo(main)
            .assignAndGet();
    soft.assertThat(ref)
        .extracting(Reference::getName, Reference::getHash)
        .containsExactly(ref0.getName(), main.getHash());

    ref =
        api.assignReference()
            .refName(ref.getName())
            .hash(ref.getHash())
            .refType(ref.getType())
            .assignTo(main1)
            .assignAndGet();
    soft.assertThat(ref)
        .extracting(Reference::getName, Reference::getHash)
        .containsExactly(ref0.getName(), main1.getHash());

    AssignReferenceBuilder<Reference> assignRequest =
        api.assignReference().refName(ref.getName()).hash(ref.getHash()).assignTo(main1);
    ref =
        referenceType == ReferenceType.BRANCH
            ? assignRequest.asBranch().assignAndGet()
            : assignRequest.asTag().assignAndGet();
    soft.assertThat(ref)
        .extracting(Reference::getName, Reference::getHash)
        .containsExactly(ref0.getName(), main1.getHash());

    soft.assertThatThrownBy(() -> api.deleteReference().refName(ref0.getName()).delete())
        .isInstanceOf(NessieBadRequestException.class)
        .hasMessageContaining("Expected hash must be provided");

    Reference deleted = api.deleteReference().reference(ref).getAndDelete();
    soft.assertThat(deleted).isEqualTo(ref);

    ref = createReference(ref0, main.getName());
    deleted = api.deleteReference().refName(ref.getName()).hash(ref.getHash()).getAndDelete();
    soft.assertThat(deleted).isEqualTo(ref);

    ref = createReference(ref0, main.getName());
    soft.assertThatThrownBy(
            () ->
                api.deleteReference()
                    .refName(ref0.getName())
                    .hash(ref0.getHash())
                    .refType(anotherType)
                    .delete())
        .isInstanceOf(NessieBadRequestException.class)
        .hasMessageMatching(".*Expected reference type .+ does not match existing reference.*");

    deleted =
        api.deleteReference()
            .refName(ref.getName())
            .hash(ref.getHash())
            .refType(ref.getType())
            .getAndDelete();
    soft.assertThat(deleted).isEqualTo(ref);

    ref = createReference(ref0, main.getName());
    DeleteReferenceBuilder<Reference> deleteRequest =
        api.deleteReference().refName(ref.getName()).hash(ref.getHash());
    deleted =
        referenceType == ReferenceType.BRANCH
            ? deleteRequest.asBranch().getAndDelete()
            : deleteRequest.asTag().getAndDelete();
    soft.assertThat(deleted).isEqualTo(ref);
  }

  @Test
  public void commitMergeTransplant() throws Exception {
    Branch main = api().getDefaultBranch();

    main =
        prepCommit(
                main,
                "common ancestor",
                dummyPut("unrelated"),
                Put.of(ContentKey.of("a"), Namespace.of("a")),
                Put.of(ContentKey.of("b"), Namespace.of("b")))
            .commit();
    main = prepCommit(main, "common ancestor", Delete.of(ContentKey.of("unrelated"))).commit();

    Branch branch = createReference(Branch.of("branch", main.getHash()), main.getName());

    Branch otherBranch = createReference(Branch.of("other", main.getHash()), main.getName());

    if (isV2()) {
      CommitResponse resp = prepCommit(branch, "one", dummyPut("a", "a")).commitWithResponse();
      branch = resp.getTargetBranch();
      soft.assertThat(resp.getAddedContents()).hasSize(1);
      resp = prepCommit(branch, "two", dummyPut("b", "a"), dummyPut("b", "b")).commitWithResponse();
      branch = resp.getTargetBranch();
      soft.assertThat(resp.getAddedContents())
          .hasSize(2)
          .extracting(AddedContent::getKey)
          .containsExactlyInAnyOrder(ContentKey.of("b", "a"), ContentKey.of("b", "b"));
      soft.assertThat(resp.contentWithAssignedId(ContentKey.of("b", "a"), dummyTable()))
          .extracting(IcebergTable::getId)
          .isNotNull();
      soft.assertThat(resp.contentWithAssignedId(ContentKey.of("b", "b"), dummyTable()))
          .extracting(IcebergTable::getId)
          .isNotNull();
      soft.assertThat(resp.contentWithAssignedId(ContentKey.of("x", "y"), dummyTable()))
          .extracting(IcebergTable::getId)
          .isNull();
    } else {
      branch = prepCommit(branch, "one", dummyPut("a", "a")).commit();
      branch = prepCommit(branch, "two", dummyPut("b", "a"), dummyPut("b", "b")).commit();
    }

    soft.assertThat(api().getCommitLog().refName(branch.getName()).get().getLogEntries())
        .hasSize(4);

    soft.assertThat(api().getEntries().reference(branch).get().getEntries())
        .extracting(Entry::getName)
        .containsExactlyInAnyOrder(
            ContentKey.of("a"),
            ContentKey.of("b"),
            ContentKey.of("a", "a"),
            ContentKey.of("b", "a"),
            ContentKey.of("b", "b"));

    soft.assertThat(api().getCommitLog().refName(main.getName()).get().getLogEntries()).hasSize(2);
    soft.assertThat(api().getEntries().reference(main).get().getEntries())
        .extracting(Entry::getName)
        .containsExactly(ContentKey.of("a"), ContentKey.of("b"));

    Reference main2;
    if (isV2()) {
      api()
          .mergeRefIntoBranch()
          .fromRef(branch)
          .branch(main)
          .message("not the merge message")
          .commitMeta(
              CommitMeta.builder()
                  .message("My custom merge message")
                  .author("NessieHerself")
                  .signedOffBy("Arctic")
                  .authorTime(Instant.EPOCH)
                  .putProperties("property", "value")
                  .build())
          .keepIndividualCommits(false)
          .merge();
      main2 = api().getReference().refName(main.getName()).get();
      List<LogEntry> postMergeLog =
          api().getCommitLog().refName(main.getName()).get().getLogEntries();
      soft.assertThat(postMergeLog)
          .hasSize(3)
          .first()
          .extracting(LogEntry::getCommitMeta)
          .extracting(
              CommitMeta::getMessage,
              CommitMeta::getAllAuthors,
              CommitMeta::getAllSignedOffBy,
              CommitMeta::getAuthorTime,
              CommitMeta::getProperties)
          .containsExactly(
              "My custom merge message",
              singletonList("NessieHerself"),
              singletonList("Arctic"),
              Instant.EPOCH,
              singletonMap("property", "value"));
    } else {
      api().mergeRefIntoBranch().fromRef(branch).branch(main).keepIndividualCommits(false).merge();
      main2 = api().getReference().refName(main.getName()).get();
      soft.assertThat(api().getCommitLog().refName(main.getName()).get().getLogEntries())
          .hasSize(3);
    }

    soft.assertThat(api().getEntries().reference(main2).get().getEntries())
        .extracting(Entry::getName)
        .containsExactlyInAnyOrder(
            ContentKey.of("a"),
            ContentKey.of("b"),
            ContentKey.of("a", "a"),
            ContentKey.of("b", "a"),
            ContentKey.of("b", "b"));

    soft.assertThat(api().getEntries().reference(otherBranch).get().getEntries())
        .extracting(Entry::getName)
        .containsExactly(ContentKey.of("a"), ContentKey.of("b"));
    api()
        .transplantCommitsIntoBranch()
        .fromRefName(main.getName())
        .hashesToTransplant(singletonList(main2.getHash()))
        .branch(otherBranch)
        .transplant();
    soft.assertThat(api().getEntries().refName(otherBranch.getName()).get().getEntries())
        .extracting(Entry::getName)
        .containsExactlyInAnyOrder(
            ContentKey.of("a"),
            ContentKey.of("b"),
            ContentKey.of("a", "a"),
            ContentKey.of("b", "a"),
            ContentKey.of("b", "b"));

    soft.assertThat(
            api()
                .getContent()
                .key(ContentKey.of("a", "a"))
                .key(ContentKey.of("b", "a"))
                .key(ContentKey.of("b", "b"))
                .refName(main.getName())
                .get())
        .containsKeys(ContentKey.of("a", "a"), ContentKey.of("b", "a"), ContentKey.of("b", "b"));
  }

  @SuppressWarnings("deprecation")
  @Test
  public void mergeTransplantDryRunWithConflictInResult() throws Exception {
    Branch main0 =
        prepCommit(api().getDefaultBranch(), "common ancestor", dummyPut("common")).commit();
    Branch branch = createReference(Branch.of("branch", main0.getHash()), main0.getName());

    branch =
        prepCommit(branch, "branch", dummyPut("conflictingKey1"), dummyPut("branchKey")).commit();
    Branch main =
        prepCommit(main0, "main", dummyPut("conflictingKey1"), dummyPut("mainKey")).commit();

    ListAssert<ContentKeyDetails> mergeAssert =
        soft.assertThat(
            api()
                .mergeRefIntoBranch()
                .fromRef(branch)
                .branch(main)
                .returnConflictAsResult(true) // adds coverage on top of AbstractMerge tests
                .dryRun(true)
                .merge()
                .getDetails());
    if (isV2()) {
      mergeAssert
          // old model returns "UNKNOWN" for Conflict.conflictType(), new model returns KEY_EXISTS
          .extracting(ContentKeyDetails::getKey, d -> d.getConflict() != null)
          .contains(
              tuple(ContentKey.of("branchKey"), false),
              tuple(ContentKey.of("conflictingKey1"), true));
    } else {
      mergeAssert
          .extracting(ContentKeyDetails::getKey, ContentKeyDetails::getConflictType)
          .contains(
              tuple(ContentKey.of("branchKey"), ContentKeyConflict.NONE),
              tuple(ContentKey.of("conflictingKey1"), ContentKeyConflict.UNRESOLVABLE));
    }

    // Assert no change to the ref HEAD
    soft.assertThat(api().getReference().refName(main.getName()).get()).isEqualTo(main);

    mergeAssert =
        soft.assertThat(
            api()
                .transplantCommitsIntoBranch()
                .fromRefName(branch.getName())
                .hashesToTransplant(singletonList(branch.getHash()))
                .branch(main0)
                .returnConflictAsResult(true) // adds coverage on top of AbstractTransplant tests
                .dryRun(true)
                .transplant()
                .getDetails());
    if (isV2()) {
      mergeAssert
          // old model returns "UNKNOWN" for Conflict.conflictType(), new model returns KEY_EXISTS
          .extracting(ContentKeyDetails::getKey, d -> d.getConflict() != null)
          .contains(
              tuple(ContentKey.of("branchKey"), false),
              tuple(ContentKey.of("conflictingKey1"), true));
    } else {
      mergeAssert
          .extracting(ContentKeyDetails::getKey, ContentKeyDetails::getConflictType)
          .contains(
              tuple(ContentKey.of("branchKey"), ContentKeyConflict.NONE),
              tuple(ContentKey.of("conflictingKey1"), ContentKeyConflict.UNRESOLVABLE));
    }
    // Assert no change to the ref HEAD
    soft.assertThat(api().getReference().refName(main.getName()).get()).isEqualTo(main);
  }

  @Test
  @NessieApiVersions(versions = NessieApiVersion.V2)
  public void commitParents() throws Exception {
    Branch main = api().getDefaultBranch();

    Branch initialCommit = prepCommit(main, "common ancestor", dummyPut("initial")).commit();
    main = prepCommit(main, "common ancestor", dummyPut("test1")).commit();

    soft.assertThat(
            api().getCommitLog().refName(main.getName()).maxRecords(1).get().getLogEntries())
        .map(logEntry -> logEntry.getCommitMeta().getParentCommitHashes())
        .first()
        .asInstanceOf(list(String.class))
        .containsExactly(initialCommit.getHash());

    Branch branch = createReference(Branch.of("branch", main.getHash()), main.getName());

    branch =
        prepCommit(branch, "one", Put.of(ContentKey.of("a"), Namespace.of("a")), dummyPut("a", "a"))
            .commit();
    Reference mainParent = api().getReference().refName(main.getName()).get();

    api().mergeRefIntoBranch().fromRef(branch).branch(main).merge();

    soft.assertThat(
            api().getCommitLog().refName(main.getName()).maxRecords(1).get().getLogEntries())
        .map(logEntry -> logEntry.getCommitMeta().getParentCommitHashes())
        .first()
        .asInstanceOf(list(String.class))
        .containsExactly(
            mainParent.getHash(), branch.getHash()); // branch lineage parent, then merge parent
  }

  @Test
  public void diff() throws Exception {
    Branch main = api().getDefaultBranch();
    Branch branch1 = createReference(Branch.of("b1", main.getHash()), main.getName());
    Branch branch2 = createReference(Branch.of("b2", main.getHash()), main.getName());

    ContentKey key1 = ContentKey.of("1");
    ContentKey key3 = ContentKey.of("3");
    ContentKey key4 = ContentKey.of("4");

    branch1 =
        prepCommit(
                branch1,
                "c1",
                Put.of(key1, Namespace.of("1")),
                dummyPut("1", "1"),
                dummyPut("1", "2"),
                dummyPut("1", "3"))
            .commit();
    branch2 =
        prepCommit(
                branch2,
                "c2",
                Put.of(key1, Namespace.of("1")),
                Put.of(key3, Namespace.of("3")),
                Put.of(key4, Namespace.of("4")),
                dummyPut("1", "1"),
                dummyPut("3", "1"),
                dummyPut("4", "1"))
            .commit();

    ContentKey key11 = ContentKey.of("1", "1");
    ContentKey key12 = ContentKey.of("1", "2");
    ContentKey key13 = ContentKey.of("1", "3");
    ContentKey key31 = ContentKey.of("3", "1");
    ContentKey key41 = ContentKey.of("4", "1");
    Map<ContentKey, Content> contents1 =
        api()
            .getContent()
            .reference(branch1)
            .key(key11)
            .key(key12)
            .key(key13)
            .key(key11.getParent())
            .key(key31.getParent())
            .key(key41.getParent())
            .get();
    Map<ContentKey, Content> contents2 =
        api()
            .getContent()
            .reference(branch2)
            .key(key11)
            .key(key31)
            .key(key41)
            .key(key11.getParent())
            .key(key31.getParent())
            .key(key41.getParent())
            .get();

    DiffResponse diff1response = api().getDiff().fromRef(branch1).toRef(branch2).get();
    List<DiffEntry> diff1 = diff1response.getDiffs();

    if (isV2()) {
      soft.assertThat(diff1response.getEffectiveFromReference()).isEqualTo(branch1);
      soft.assertThat(diff1response.getEffectiveToReference()).isEqualTo(branch2);

      // Key filtering
      soft.assertThat(
              api()
                  .getDiff()
                  .fromRef(branch1)
                  .toRef(branch2)
                  .minKey(key12)
                  .maxKey(key31)
                  .get()
                  .getDiffs())
          .extracting(DiffEntry::getKey)
          .containsExactlyInAnyOrder(key12, key13, key3, key31);
      soft.assertThat(
              api().getDiff().fromRef(branch1).toRef(branch2).minKey(key31).get().getDiffs())
          .extracting(DiffEntry::getKey)
          .containsExactlyInAnyOrder(key31, key4, key41);
      soft.assertThat(
              api().getDiff().fromRef(branch1).toRef(branch2).maxKey(key12).get().getDiffs())
          .extracting(DiffEntry::getKey)
          .containsExactlyInAnyOrder(key1, key11, key12);

      soft.assertThat(api().getDiff().fromRef(branch1).toRef(branch2).key(key12).get().getDiffs())
          .extracting(DiffEntry::getKey)
          .containsExactlyInAnyOrder(key12);
      soft.assertThat(
              api()
                  .getDiff()
                  .fromRef(branch1)
                  .toRef(branch2)
                  .key(key31)
                  .key(key12)
                  .get()
                  .getDiffs())
          .extracting(DiffEntry::getKey)
          .containsExactlyInAnyOrder(key12, key31);
      soft.assertThat(
              api()
                  .getDiff()
                  .fromRef(branch1)
                  .toRef(branch2)
                  .filter("key.namespace=='1'")
                  .get()
                  .getDiffs())
          .extracting(DiffEntry::getKey)
          .containsExactlyInAnyOrder(key11, key12, key13);
    }
    soft.assertThat(diff1)
        .containsExactlyInAnyOrder(
            DiffEntry.diffEntry(
                key11.getParent(),
                contents1.get(key11.getParent()),
                contents2.get(key11.getParent())),
            DiffEntry.diffEntry(key11, contents1.get(key11), contents2.get(key11)),
            DiffEntry.diffEntry(key12, contents1.get(key12), null),
            DiffEntry.diffEntry(key13, contents1.get(key13), null),
            DiffEntry.diffEntry(key31, null, contents2.get(key31)),
            DiffEntry.diffEntry(key41, null, contents2.get(key41)),
            DiffEntry.diffEntry(key31.getParent(), null, contents2.get(key31.getParent())),
            DiffEntry.diffEntry(key41.getParent(), null, contents2.get(key41.getParent())));

    List<DiffEntry> diff2 =
        api().getDiff().fromRefName(branch1.getName()).toRef(branch2).get().getDiffs();
    List<DiffEntry> diff3 =
        api().getDiff().fromRef(branch1).toRefName(branch2.getName()).get().getDiffs();
    soft.assertThat(diff1).isEqualTo(diff2).isEqualTo(diff3);

    // Paging
    if (isV2()) {
      List<DiffEntry> all = new ArrayList<>();
      String token = null;
      for (int i = 0; i < 8; i++) {
        DiffResponse resp =
            api().getDiff().fromRef(branch1).toRef(branch2).maxRecords(1).pageToken(token).get();
        all.addAll(resp.getDiffs());
        token = resp.getToken();
        if (i == 7) {
          soft.assertThat(token).isNull();
        } else {
          soft.assertThat(token).isNotNull();
        }
      }

      soft.assertThat(all).containsExactlyInAnyOrderElementsOf(diff1);

      soft.assertThat(api().getDiff().fromRef(branch1).toRef(branch2).maxRecords(1).stream())
          .containsExactlyInAnyOrderElementsOf(diff1);
    }
  }

  @Test
  public void commitLog() throws Exception {
    Branch main = api().getDefaultBranch();
    for (int i = 0; i < 10; i++) {
      if (i == 0) {
        main =
            prepCommit(
                    main,
                    "c-" + i,
                    Put.of(ContentKey.of("c"), Namespace.of("c")),
                    dummyPut("c", Integer.toString(i)))
                .commit();
      } else {
        main = prepCommit(main, "c-" + i, dummyPut("c", Integer.toString(i))).commit();
      }
    }

    List<LogEntry> notPaged = api().getCommitLog().reference(main).get().getLogEntries();
    soft.assertThat(notPaged).hasSize(10);

    List<LogEntry> all = new ArrayList<>();
    String token = null;
    for (int i = 0; i < 10; i++) {
      LogResponse resp = api().getCommitLog().reference(main).maxRecords(1).pageToken(token).get();
      all.addAll(resp.getLogEntries());
      token = resp.getToken();
      if (i == 9) {
        soft.assertThat(token).isNull();
      } else {
        soft.assertThat(token).isNotNull();
      }
    }

    soft.assertThat(all).containsExactlyElementsOf(notPaged);

    soft.assertAll();

    soft.assertThat(api().getCommitLog().reference(main).maxRecords(1).stream())
        .containsExactlyInAnyOrderElementsOf(all);
  }

  @Test
  public void allReferences() throws Exception {
    Branch main = api().getDefaultBranch();

    List<Reference> expect = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      expect.add(createReference(Branch.of("b-" + i, main.getHash()), main.getName()));
      expect.add(createReference(Tag.of("t-" + i, main.getHash()), main.getName()));
    }
    expect.add(main);

    List<Reference> notPaged = api().getAllReferences().get().getReferences();
    soft.assertThat(notPaged).containsExactlyInAnyOrderElementsOf(expect);

    List<Reference> all = new ArrayList<>();
    String token = null;
    for (int i = 0; i < 11; i++) {
      ReferencesResponse resp = api().getAllReferences().maxRecords(1).pageToken(token).get();
      all.addAll(resp.getReferences());
      token = resp.getToken();
      if (i == 10) {
        soft.assertThat(token).isNull();
      } else {
        soft.assertThat(token).isNotNull();
      }
    }

    soft.assertThat(all).containsExactlyElementsOf(notPaged);

    soft.assertAll();

    soft.assertThat(api().getAllReferences().maxRecords(1).stream())
        .containsExactlyInAnyOrderElementsOf(all);
  }

  @Test
  @NessieApiVersions(versions = NessieApiVersion.V1)
  public void contentsOnDefaultBranch() throws Exception {
    Branch main = api().getDefaultBranch();
    ContentKey key = ContentKey.of("test.key1");
    Branch main1 =
        prepCommit(main, "commit", Put.of(key, IcebergTable.of("loc111", 1, 2, 3, 4))).commit();
    // Note: not specifying a reference name in API v1 means using the HEAD of the default branch.
    IcebergTable created = (IcebergTable) api().getContent().key(key).get().get(key);
    assertThat(created.getMetadataLocation()).isEqualTo("loc111");
    prepCommit(
            main1,
            "commit",
            Put.of(key, IcebergTable.builder().from(created).metadataLocation("loc222").build()))
        .commit();
    IcebergTable table2 = (IcebergTable) api().getContent().key(key).get().get(key);
    assertThat(table2.getMetadataLocation()).isEqualTo("loc222");
    // Note: not specifying a reference name for the default branch, but setting an older hash.
    IcebergTable table1 =
        (IcebergTable) api().getContent().hashOnRef(main1.getHash()).key(key).get().get(key);
    assertThat(table1).isEqualTo(created);
  }

  @Test
  @NessieApiVersions(versions = NessieApiVersion.V2)
  public void contents() throws Exception {
    Branch main = api().getDefaultBranch();
    CommitResponse committed =
        prepCommit(
                main,
                "commit",
                Stream.concat(
                        Stream.of(
                            Put.of(ContentKey.of("b.b"), Namespace.of("b.b")),
                            Put.of(ContentKey.of("b.b", "c"), Namespace.of("b.b", "c"))),
                        IntStream.range(0, 8)
                            .mapToObj(i -> dummyPut("b.b", "c", Integer.toString(i))))
                    .toArray(Operation[]::new))
            .commitWithResponse();
    main = committed.getTargetBranch();

    List<ContentKey> allKeys =
        Stream.concat(
                Stream.of(ContentKey.of("b.b"), ContentKey.of("b.b", "c")),
                IntStream.range(0, 8).mapToObj(i -> ContentKey.of("b.b", "c", Integer.toString(i))))
            .collect(Collectors.toList());

    GetMultipleContentsResponse resp =
        api().getContent().refName(main.getName()).keys(allKeys).getWithResponse();

    ContentKey singleKey = ContentKey.of("b.b", "c", "3");
    soft.assertThat(api().getContent().refName(main.getName()).getSingle(singleKey))
        .extracting(ContentResponse::getEffectiveReference, ContentResponse::getContent)
        .containsExactly(
            main,
            IcebergTable.of("foo", 1, 2, 3, 4, committed.toAddedContentsMap().get(singleKey)));

    soft.assertThat(resp.getEffectiveReference()).isEqualTo(main);
    soft.assertThat(resp.toContentsMap()).containsOnlyKeys(allKeys).hasSize(allKeys.size());

    String mainName = main.getName();
    ContentKey key = ContentKey.of("b.b", "c", "1");
    soft.assertThat(api().getContent().refName(mainName).getSingle(key))
        .isEqualTo(
            ContentResponse.of(
                IcebergTable.of("foo", 1, 2, 3, 4, committed.toAddedContentsMap().get(key)), main));

    ContentKey nonExisting = ContentKey.of("not", "there");
    soft.assertThatThrownBy(() -> api().getContent().refName(mainName).getSingle(nonExisting))
        .isInstanceOf(NessieContentNotFoundException.class)
        .asInstanceOf(type(NessieContentNotFoundException.class))
        .extracting(NessieContentNotFoundException::getErrorDetails)
        .extracting(ContentKeyErrorDetails::contentKey)
        .isEqualTo(nonExisting);
  }

  @Test
  public void entries() throws Exception {
    Branch main0 = api().getDefaultBranch();

    Branch main =
        prepCommit(
                main0,
                "commit",
                Stream.concat(
                        Stream.of(Put.of(ContentKey.of("c"), Namespace.of("c"))),
                        IntStream.range(0, 9).mapToObj(i -> dummyPut("c", Integer.toString(i))))
                    .toArray(Put[]::new))
            .commit();

    EntriesResponse response = api().getEntries().reference(main).withContent(isV2()).get();
    List<Entry> notPaged = response.getEntries();
    soft.assertThat(notPaged).hasSize(10);
    if (isV2()) {
      soft.assertThat(response.getEffectiveReference()).isEqualTo(main);
      soft.assertThat(response.getEntries())
          .extracting(Entry::getContent)
          .doesNotContainNull()
          .isNotEmpty();

      soft.assertThat(
              api()
                  .getEntries()
                  .reference(main)
                  .minKey(ContentKey.of("c", "2"))
                  .maxKey(ContentKey.of("c", "4"))
                  .get()
                  .getEntries())
          .extracting(Entry::getName)
          .containsExactlyInAnyOrder(
              ContentKey.of("c", "2"), ContentKey.of("c", "3"), ContentKey.of("c", "4"));
      soft.assertThat(
              api().getEntries().reference(main).prefixKey(ContentKey.of("c")).get().getEntries())
          .extracting(Entry::getName)
          .contains(ContentKey.of("c"))
          .containsAll(
              IntStream.range(0, 9)
                  .mapToObj(i -> ContentKey.of("c", Integer.toString(i)))
                  .collect(Collectors.toList()));
      soft.assertThat(
              api()
                  .getEntries()
                  .reference(main)
                  .key(ContentKey.of("c", "2"))
                  .key(ContentKey.of("c", "4"))
                  .get()
                  .getEntries())
          .extracting(Entry::getName)
          .containsExactlyInAnyOrder(ContentKey.of("c", "2"), ContentKey.of("c", "4"));
      soft.assertThat(
              api()
                  .getEntries()
                  .reference(main)
                  .prefixKey(ContentKey.of("c", "5"))
                  .get()
                  .getEntries())
          .extracting(Entry::getName)
          .containsExactlyInAnyOrder(ContentKey.of("c", "5"));
    }

    List<Entry> all = new ArrayList<>();
    String token = null;
    for (int i = 0; i < 10; i++) {
      EntriesResponse resp =
          api()
              .getEntries()
              .withContent(isV2())
              .reference(main)
              .maxRecords(1)
              .pageToken(token)
              .get();
      all.addAll(resp.getEntries());
      token = resp.getToken();
      if (i == 9) {
        soft.assertThat(token).isNull();
      } else {
        soft.assertThat(token).isNotNull();
      }
    }

    soft.assertThat(all).containsExactlyElementsOf(notPaged);

    soft.assertAll();

    soft.assertThat(api().getEntries().withContent(isV2()).reference(main).maxRecords(1).stream())
        .containsExactlyInAnyOrderElementsOf(all);
  }

  @NessieApiVersions(versions = NessieApiVersion.V2)
  @Test
  public void entryContentId() throws Exception {
    Branch main = prepCommit(api().getDefaultBranch(), "commit", dummyPut("test-table")).commit();

    soft.assertThat(api().getEntries().reference(main).stream())
        .isNotEmpty()
        .allSatisfy(e -> assertThat(e.getContentId()).isNotNull());
  }

  @Test
  public void udf() throws Exception {
    ContentKey key = ContentKey.of("test-udf");
    Branch main =
        prepCommit(api().getDefaultBranch(), "commit", Put.of(key, UDF.udf("loc1", "v1", "s1")))
            .commit();

    soft.assertThat(api().getContent().reference(main).key(key).get().get(key))
        .asInstanceOf(type(UDF.class))
        .satisfies(u -> assertThat(u.getId()).isNotNull())
        .extracting(UDF::getMetadataLocation, UDF::getVersionId, UDF::getSignatureId)
        .containsExactly("loc1", "v1", "s1");

    soft.assertThat(api().getEntries().reference(main).stream())
        .hasSize(1)
        .element(0)
        .extracting(Entry::getType, Entry::getName)
        .containsExactly(Content.Type.UDF, key);
  }

  @Test
  public void namespaces() throws Exception {
    Branch main = api().getDefaultBranch();
    String mainName = main.getName();

    soft.assertThat(
            api()
                .getMultipleNamespaces()
                .reference(main)
                .namespace(EMPTY_NAMESPACE)
                .get()
                .getNamespaces())
        .isEmpty();

    Namespace namespace1 = Namespace.of("a");
    Namespace namespace2 = Namespace.of("a", "b.b");
    Namespace namespace3 = Namespace.of("a", "b.bbbb");
    Namespace namespace4 = Namespace.of("a", "b.b", "c");
    Namespace namespace1WithId;
    Namespace namespace2WithId;
    Namespace namespace3WithId;
    Namespace namespace4WithId;

    Function<String, CommitMeta> buildMeta =
        msg ->
            CommitMeta.builder()
                .authorTime(Instant.EPOCH)
                .author("NessieHerself")
                .signedOffBy("Arctic")
                .message(msg + " my namespace with commit meta")
                .putProperties("property", "value")
                .build();
    BiConsumer<Reference, String> checkMeta =
        (ref, msg) -> {
          try (Stream<LogEntry> log = api().getCommitLog().reference(ref).maxRecords(1).stream()) {
            soft.assertThat(log)
                .first()
                .extracting(LogEntry::getCommitMeta)
                .extracting(
                    CommitMeta::getMessage,
                    CommitMeta::getAllAuthors,
                    CommitMeta::getAuthorTime,
                    CommitMeta::getAllSignedOffBy,
                    CommitMeta::getProperties)
                .containsExactly(
                    msg + " my namespace with commit meta",
                    singletonList("NessieHerself"),
                    Instant.EPOCH,
                    singletonList("Arctic"),
                    singletonMap("property", "value"));
          } catch (NessieNotFoundException e) {
            throw new RuntimeException(e);
          }
        };

    if (isV2()) {
      CreateNamespaceResult resp1 =
          api()
              .createNamespace()
              .refName(mainName)
              .namespace(namespace1)
              .commitMeta(buildMeta.apply("Create"))
              .createWithResponse();
      soft.assertThat(resp1.getEffectiveBranch()).isNotNull().isNotEqualTo(main);
      checkMeta.accept(resp1.getEffectiveBranch(), "Create");

      CreateNamespaceResult resp2 =
          api().createNamespace().refName(mainName).namespace(namespace2).createWithResponse();
      soft.assertThat(resp2.getEffectiveBranch())
          .isNotNull()
          .isNotEqualTo(main)
          .isNotEqualTo(resp1.getEffectiveBranch());
      CreateNamespaceResult resp3 =
          api().createNamespace().refName(mainName).namespace(namespace3).createWithResponse();
      soft.assertThat(resp3.getEffectiveBranch())
          .isNotNull()
          .isNotEqualTo(resp1.getEffectiveBranch())
          .isNotEqualTo(resp2.getEffectiveBranch());
      CreateNamespaceResult resp4 =
          api().createNamespace().refName(mainName).namespace(namespace4).createWithResponse();
      soft.assertThat(resp4.getEffectiveBranch())
          .isNotNull()
          .isNotEqualTo(resp2.getEffectiveBranch())
          .isNotEqualTo(resp3.getEffectiveBranch());
      namespace1WithId = resp1.getNamespace();
      namespace2WithId = resp2.getNamespace();
      namespace3WithId = resp3.getNamespace();
      namespace4WithId = resp4.getNamespace();

      for (Map.Entry<Namespace, List<Namespace>> c :
          ImmutableMap.<Namespace, List<Namespace>>of(
                  EMPTY_NAMESPACE,
                  singletonList(namespace1WithId),
                  namespace1,
                  asList(namespace2WithId, namespace3WithId),
                  namespace2,
                  singletonList(namespace4WithId),
                  namespace3,
                  emptyList(),
                  namespace4,
                  emptyList())
              .entrySet()) {
        soft.assertThat(
                api()
                    .getMultipleNamespaces()
                    .refName(mainName)
                    .namespace(c.getKey())
                    .onlyDirectChildren(true)
                    .get()
                    .getNamespaces())
            .describedAs("for namespace %s", c.getKey())
            .containsExactlyInAnyOrderElementsOf(c.getValue());
      }
    } else {
      namespace1WithId = api().createNamespace().refName(mainName).namespace(namespace1).create();
      namespace2WithId = api().createNamespace().refName(mainName).namespace(namespace2).create();
      namespace3WithId = api().createNamespace().refName(mainName).namespace(namespace3).create();
      namespace4WithId = api().createNamespace().refName(mainName).namespace(namespace4).create();
    }

    // Add some non-namespace content to check that it is not returned by the namespace API
    IcebergTable table = IcebergTable.of("irrelevant", 1, 2, 3, 4);
    ContentKey table1 = ContentKey.of(namespace4, "table1");
    api()
        .commitMultipleOperations()
        .branch(main)
        .commitMeta(CommitMeta.fromMessage("Add table1"))
        .operation(Put.of(table1, table))
        .commit();

    for (Map.Entry<Namespace, List<Namespace>> c :
        ImmutableMap.of(
                EMPTY_NAMESPACE,
                asList(namespace1WithId, namespace2WithId, namespace3WithId, namespace4WithId),
                namespace1,
                asList(namespace1WithId, namespace2WithId, namespace3WithId, namespace4WithId),
                namespace2,
                asList(namespace2WithId, namespace4WithId),
                namespace3,
                singletonList(namespace3WithId),
                namespace4,
                singletonList(namespace4WithId))
            .entrySet()) {
      soft.assertThat(
              api()
                  .getMultipleNamespaces()
                  .refName(mainName)
                  .namespace(c.getKey())
                  .get()
                  .getNamespaces())
          .describedAs("for namespace %s", c.getKey())
          .containsExactlyInAnyOrderElementsOf(c.getValue());
    }

    GetNamespacesResponse getMultiple =
        api().getMultipleNamespaces().refName(mainName).namespace(EMPTY_NAMESPACE).get();
    if (isV2()) {
      main = (Branch) api().getReference().refName(mainName).get();
      soft.assertThat(getMultiple.getEffectiveReference()).isEqualTo(main);
    }
    soft.assertThat(getMultiple.getNamespaces())
        .containsExactlyInAnyOrder(
            namespace1WithId, namespace2WithId, namespace3WithId, namespace4WithId);
    soft.assertThat(
            api()
                .getMultipleNamespaces()
                .refName(mainName)
                .namespace(namespace1)
                .get()
                .getNamespaces())
        .containsExactlyInAnyOrder(
            namespace1WithId, namespace2WithId, namespace3WithId, namespace4WithId);
    soft.assertThat(
            api()
                .getMultipleNamespaces()
                .refName(mainName)
                .namespace(namespace2)
                .get()
                .getNamespaces())
        .containsExactlyInAnyOrder(namespace2WithId, namespace4WithId);

    soft.assertThat(
            api()
                .getContent()
                .refName(mainName)
                .key(namespace1.toContentKey())
                .key(namespace2.toContentKey())
                .key(namespace3.toContentKey())
                .key(namespace4.toContentKey())
                .get())
        .containsEntry(namespace1.toContentKey(), namespace1WithId)
        .containsEntry(namespace2.toContentKey(), namespace2WithId)
        .containsEntry(namespace3.toContentKey(), namespace3WithId)
        .containsEntry(namespace4.toContentKey(), namespace4WithId);

    soft.assertThat(api().getNamespace().refName(mainName).namespace(namespace1).get())
        .isEqualTo(namespace1WithId);
    soft.assertThat(api().getNamespace().refName(mainName).namespace(namespace2).get())
        .isEqualTo(namespace2WithId);
    soft.assertThat(api().getNamespace().refName(mainName).namespace(namespace3).get())
        .isEqualTo(namespace3WithId);
    soft.assertThat(api().getNamespace().refName(mainName).namespace(namespace4).get())
        .isEqualTo(namespace4WithId);

    if (isV2()) {
      main = (Branch) api().getReference().refName(mainName).get();
      UpdateNamespaceResult update =
          api()
              .updateProperties()
              .refName(mainName)
              .namespace(namespace2)
              .commitMeta(buildMeta.apply("Update"))
              .updateProperty("foo", "bar")
              .updateProperty("bar", "baz")
              .updateWithResponse();
      soft.assertThat(update.getEffectiveBranch()).isNotNull().isNotEqualTo(main);
      checkMeta.accept(update.getEffectiveBranch(), "Update");
    } else {
      api()
          .updateProperties()
          .refName(mainName)
          .namespace(namespace2)
          .updateProperty("foo", "bar")
          .updateProperty("bar", "baz")
          .update();
    }
    Namespace namespace2update =
        (Namespace)
            api()
                .getContent()
                .refName(mainName)
                .key(ContentKey.of("a", "b.b"))
                .get()
                .get(ContentKey.of("a", "b.b"));
    soft.assertThat(api().getNamespace().refName(mainName).namespace(namespace2).get())
        .isEqualTo(namespace2update);

    soft.assertThat(
            api()
                .getMultipleNamespaces()
                .refName(mainName)
                .namespace(EMPTY_NAMESPACE)
                .get()
                .getNamespaces())
        .containsExactlyInAnyOrder(
            namespace1WithId, namespace2update, namespace3WithId, namespace4WithId);

    if (isV2()) {
      UpdateNamespaceResult updateResponse =
          api()
              .updateProperties()
              .refName(mainName)
              .namespace(namespace2)
              .removeProperty("foo")
              .updateWithResponse();
      soft.assertThat(updateResponse.getEffectiveBranch()).isNotEqualTo(main);
      updateResponse.getEffectiveBranch();
    } else {
      api()
          .updateProperties()
          .refName(mainName)
          .namespace(namespace2)
          .removeProperty("foo")
          .update();
    }
    Namespace namespace2update2 =
        (Namespace)
            api()
                .getContent()
                .refName(mainName)
                .key(ContentKey.of("a", "b.b"))
                .get()
                .get(ContentKey.of("a", "b.b"));
    soft.assertThat(api().getNamespace().refName(mainName).namespace(namespace2).get())
        .isEqualTo(namespace2update2);

    soft.assertThat(
            api()
                .getMultipleNamespaces()
                .refName(mainName)
                .namespace(EMPTY_NAMESPACE)
                .get()
                .getNamespaces())
        .containsExactlyInAnyOrder(
            namespace1WithId, namespace2update2, namespace3WithId, namespace4WithId);

    if (isV2()) {
      // contains other namespaces
      soft.assertThatThrownBy(
              () -> api().deleteNamespace().refName(mainName).namespace(namespace2).delete())
          .isInstanceOf(NessieNamespaceNotEmptyException.class)
          .asInstanceOf(type(NessieNamespaceNotEmptyException.class))
          .extracting(NessieNamespaceNotEmptyException::getErrorDetails)
          .extracting(ContentKeyErrorDetails::contentKey)
          .isEqualTo(namespace2.toContentKey());
      // contains a table
      soft.assertThatThrownBy(
              () -> api().deleteNamespace().refName(mainName).namespace(namespace4).delete())
          .isInstanceOf(NessieNamespaceNotEmptyException.class)
          .asInstanceOf(type(NessieNamespaceNotEmptyException.class))
          .extracting(NessieNamespaceNotEmptyException::getErrorDetails)
          .extracting(ContentKeyErrorDetails::contentKey)
          .isEqualTo(namespace4.toContentKey());
    } else {
      soft.assertThatThrownBy(
              () -> api().deleteNamespace().refName(mainName).namespace(namespace2).delete())
          .isInstanceOf(NessieNamespaceNotEmptyException.class);
      soft.assertThatThrownBy(
              () -> api().deleteNamespace().refName(mainName).namespace(namespace4).delete())
          .isInstanceOf(NessieNamespaceNotEmptyException.class);
    }

    main = (Branch) api().getReference().refName(mainName).get();
    api()
        .commitMultipleOperations()
        .branch(main)
        .commitMeta(CommitMeta.fromMessage("Delete table1"))
        .operation(Delete.of(table1))
        .commit();

    if (isV2()) {
      main = (Branch) api().getReference().refName(mainName).get();
      DeleteNamespaceResult response =
          api()
              .deleteNamespace()
              .refName(mainName)
              .namespace(namespace4)
              .commitMeta(buildMeta.apply("Delete"))
              .deleteWithResponse();
      soft.assertThat(response.getEffectiveBranch()).isNotNull().isNotEqualTo(main);
      checkMeta.accept(response.getEffectiveBranch(), "Delete");
    } else {
      api().deleteNamespace().refName(mainName).namespace(namespace4).delete();
    }

    soft.assertThat(api().getContent().refName(mainName).key(ContentKey.of("a", "b.b", "c")).get())
        .isEmpty();

    soft.assertThatThrownBy(
            () -> api().getNamespace().refName(mainName).namespace(namespace4).get())
        .isInstanceOf(NessieNamespaceNotFoundException.class)
        .asInstanceOf(type(NessieNamespaceNotFoundException.class))
        .extracting(NessieNamespaceNotFoundException::getErrorDetails)
        .extracting(ContentKeyErrorDetails::contentKey)
        .isEqualTo(namespace4.toContentKey());

    soft.assertThatThrownBy(
            () -> api().deleteNamespace().refName(mainName).namespace(namespace4).delete())
        .isInstanceOf(NessieNamespaceNotFoundException.class);

    soft.assertThat(
            api()
                .getMultipleNamespaces()
                .refName(mainName)
                .namespace(EMPTY_NAMESPACE)
                .get()
                .getNamespaces())
        .containsExactlyInAnyOrder(namespace1WithId, namespace2update2, namespace3WithId);

    // This one fails, if the namespace-path 'startswith' filter (REST v2) to check for child
    // content is incorrectly implemented.
    soft.assertThatCode(
            () -> api().deleteNamespace().refName(mainName).namespace(namespace2).delete())
        .doesNotThrowAnyException();
  }

  @Test
  @NessieApiVersions(versions = NessieApiVersion.V2)
  public void clientSideGetNamespaces() throws BaseNessieClientServerException {
    Branch main = api().getDefaultBranch();
    String mainName = main.getName();

    Namespace parent = Namespace.of("parent");
    parent =
        api()
            .createNamespace()
            .refName(mainName)
            .namespace(parent)
            .createWithResponse()
            .getNamespace();

    // Test that effective reference is present, even if there are no child namespaces returned
    GetNamespacesResponse getMultiple =
        api().getMultipleNamespaces().refName(mainName).namespace(parent).get();
    main = (Branch) api().getReference().refName(mainName).get();
    soft.assertThat(getMultiple.getNamespaces()).containsOnly(parent);
    soft.assertThat(getMultiple.getEffectiveReference()).isEqualTo(main);

    // Test that effective reference is present, even if there are no namespaces at all returned
    getMultiple =
        api()
            .getMultipleNamespaces()
            .refName(mainName)
            .namespace(Namespace.of("nonexistent"))
            .get();
    main = (Branch) api().getReference().refName(mainName).get();
    soft.assertThat(getMultiple.getNamespaces()).isEmpty();
    soft.assertThat(getMultiple.getEffectiveReference()).isEqualTo(main);

    // Test pagination in ClientSideGetMultipleNamespaces
    for (int i = 0; i < 200; i++) {
      Namespace ns = Namespace.of("parent", "child" + i);
      api().createNamespace().refName(mainName).namespace(ns).createWithResponse();
    }
    main = (Branch) api().getReference().refName(mainName).get();
    getMultiple = api().getMultipleNamespaces().refName(mainName).namespace(parent).get();
    soft.assertThat(getMultiple.getEffectiveReference()).isEqualTo(main);
    soft.assertThat(getMultiple.getNamespaces()).hasSize(201);
  }

  @Test
  @NessieApiVersions(versions = NessieApiVersion.V2)
  public void commitLogForNamelessReference() throws BaseNessieClientServerException {
    Branch main = api().getDefaultBranch();
    Branch branch =
        createReference(Branch.of("commitLogForNamelessReference", main.getHash()), main.getName());
    for (int i = 0; i < 5; i++) {
      if (i == 0) {
        branch =
            prepCommit(
                    branch,
                    "c-" + i,
                    Put.of(ContentKey.of("c"), Namespace.of("c")),
                    dummyPut("c", Integer.toString(i)))
                .commit();
      } else {
        branch = prepCommit(branch, "c-" + i, dummyPut("c", Integer.toString(i))).commit();
      }
    }
    List<LogEntry> log =
        api().getCommitLog().hashOnRef(branch.getHash()).stream().collect(Collectors.toList());
    // Verifying size is sufficient to make sure the right log was retrieved
    assertThat(log).hasSize(5);
  }

  @Test
  @NessieApiVersions(versions = NessieApiVersion.V2)
  public void testDiffByNamelessReference() throws BaseNessieClientServerException {
    Branch main = api().getDefaultBranch();
    Branch fromRef = createReference(Branch.of("testFrom", main.getHash()), main.getName());
    Branch toRef = createReference(Branch.of("testTo", main.getHash()), main.getName());
    toRef = prepCommit(toRef, "commit", dummyPut("c")).commit();

    soft.assertThat(api().getDiff().fromRef(fromRef).toHashOnRef(toRef.getHash()).get().getDiffs())
        .hasSize(1)
        .allSatisfy(
            diff -> {
              assertThat(diff.getKey()).isNotNull();
              assertThat(diff.getFrom()).isNull();
              assertThat(diff.getTo()).isNotNull();
            });

    // both nameless references
    soft.assertThat(
            api()
                .getDiff()
                .fromHashOnRef(fromRef.getHash())
                .toHashOnRef(toRef.getHash())
                .get()
                .getDiffs())
        .hasSize(1)
        .allSatisfy(
            diff -> {
              assertThat(diff.getKey()).isNotNull();
              assertThat(diff.getFrom()).isNull();
              assertThat(diff.getTo()).isNotNull();
            });

    // reverse to/from
    soft.assertThat(api().getDiff().fromHashOnRef(toRef.getHash()).toRef(fromRef).get().getDiffs())
        .hasSize(1)
        .allSatisfy(
            diff -> {
              assertThat(diff.getKey()).isNotNull();
              assertThat(diff.getFrom()).isNotNull();
              assertThat(diff.getTo()).isNull();
            });
  }

  @Test
  @NessieApiVersions(versions = NessieApiVersion.V2)
  public void fetchEntriesByNamelessReference() throws BaseNessieClientServerException {
    Branch main = api().getDefaultBranch();
    Branch branch =
        createReference(
            Branch.of("fetchEntriesByNamelessReference", main.getHash()), main.getName());
    ContentKey a = ContentKey.of("a");
    ContentKey b = ContentKey.of("b");
    IcebergTable ta = IcebergTable.of("path1", 42, 42, 42, 42);
    IcebergView tb = IcebergView.of("pathx", 1, 1);
    branch =
        api()
            .commitMultipleOperations()
            .branch(branch)
            .operation(Put.of(a, ta))
            .operation(Put.of(b, tb))
            .commitMeta(CommitMeta.fromMessage("commit 1"))
            .commit();
    List<Entry> entries = api().getEntries().hashOnRef(branch.getHash()).get().getEntries();
    soft.assertThat(entries)
        .map(e -> immutableEntry(e.getName(), e.getType()))
        .containsExactlyInAnyOrder(
            immutableEntry(a, Content.Type.ICEBERG_TABLE),
            immutableEntry(b, Content.Type.ICEBERG_VIEW));
  }

  @Test
  @NessieApiVersions(versions = NessieApiVersion.V2)
  @DisabledOnOs(OS.WINDOWS) // time resolution issues
  public void relativeCommitLocations() throws BaseNessieClientServerException {
    Branch main = api().getDefaultBranch();

    int numCommits = 3;

    Branch branch =
        createReference(Branch.of("relativeCommitLocations", main.getHash()), main.getName());
    ContentKey key = ContentKey.of("foo");
    ImmutableIcebergTable content =
        (ImmutableIcebergTable) IcebergTable.of("here", numCommits - 1, 2, 3, 4);

    List<String> hashes = new ArrayList<>();

    for (int i = 0; i < numCommits; i++) {
      CommitResponse commitResponse =
          api.commitMultipleOperations()
              .branch(branch)
              .operation(Put.of(key, content))
              .operation(
                  Put.of(ContentKey.of("other-" + i), IcebergTable.of("other-" + i, 1, 2, 3, 4)))
              .commitMeta(fromMessage("commit " + i))
              .commitWithResponse();
      content =
          commitResponse
              .contentWithAssignedId(key, content)
              .withSnapshotId(content.getSnapshotId() - 1);
      branch = commitResponse.getTargetBranch();
      hashes.add(branch.getHash());
    }
    Collections.reverse(hashes);

    List<LogEntry> commits =
        api().getCommitLog().reference(branch).stream().collect(Collectors.toList());

    String headCommit = commits.get(0).getCommitMeta().getHash();

    for (int i = 1; i < numCommits; i++) {
      CommitMeta refCommit = commits.get(i - 1).getCommitMeta();

      String[] relativeCommitSpecs =
          new String[] {
            // n-th predecessor
            "~" + i,
            headCommit + "~" + i,
            // timestamp
            "*" + refCommit.getCommitTime().minus(1, ChronoUnit.NANOS),
            headCommit + "*" + refCommit.getCommitTime().minus(1, ChronoUnit.NANOS)
          };

      String branchName = branch.getName();
      int i2 = i;

      for (String relativeCommitSpec : relativeCommitSpecs) {

        // Check commit-log
        soft.assertThatCode(
                () ->
                    assertThat(
                            api()
                                .getCommitLog()
                                .refName(branchName)
                                .hashOnRef(relativeCommitSpec)
                                .maxRecords(1)
                                .stream()
                                .findFirst())
                        .map(LogEntry::getCommitMeta)
                        .map(CommitMeta::getHash)
                        .get()
                        .isEqualTo(hashes.get(i2)))
            .describedAs(
                "commit-log - %s - relative-commit-spec %s - ref-commit: %s %s",
                i, relativeCommitSpec, refCommit.getHash(), refCommit.getCommitTime())
            .doesNotThrowAnyException();

        // Check get-entries
        soft.assertThatCode(
                () ->
                    assertThat(
                            api()
                                .getEntries()
                                .refName(branchName)
                                .hashOnRef(relativeCommitSpec)
                                .stream()
                                .count())
                        .isEqualTo(1 + numCommits - i2))
            .describedAs(
                "get-entries - %s - relative-commit-spec %s - ref-commit: %s %s",
                i, relativeCommitSpec, refCommit.getHash(), refCommit.getCommitTime())
            .doesNotThrowAnyException();

        // Check get-content
        soft.assertThatCode(
                () -> {
                  ContentResponse cr =
                      api()
                          .getContent()
                          .refName(branchName)
                          .hashOnRef(relativeCommitSpec)
                          .getSingle(key);
                  assertThat(cr.getEffectiveReference().getHash())
                      .isEqualTo(hashes.get(i2))
                      .isEqualTo(commits.get(i2).getCommitMeta().getHash());
                  assertThat(((IcebergTable) cr.getContent()).getSnapshotId()).isEqualTo(i2);
                })
            .describedAs(
                "get-content - %s - relative-commit-spec %s - ref-commit: %s %s",
                i, relativeCommitSpec, refCommit.getHash(), refCommit.getCommitTime());
      }
    }
  }

  @Test
  @NessieApiVersions(versions = NessieApiVersion.V2)
  public void createAndUpdateRepositoryConfig() throws Exception {
    @SuppressWarnings("resource")
    NessieApiV2 api = apiV2();

    RepositoryConfig created =
        GarbageCollectorConfig.builder()
            .defaultCutoffPolicy("P30D")
            .newFilesGracePeriod(Duration.of(3, ChronoUnit.HOURS))
            .build();
    RepositoryConfig updated =
        GarbageCollectorConfig.builder()
            .defaultCutoffPolicy("P10D")
            .expectedFileCountPerContent(123)
            .build();

    soft.assertThat(created.getType()).isEqualTo(RepositoryConfig.Type.GARBAGE_COLLECTOR);
    soft.assertThat(updated.getType()).isEqualTo(RepositoryConfig.Type.GARBAGE_COLLECTOR);

    soft.assertThat(
            api.getRepositoryConfig()
                .type(RepositoryConfig.Type.GARBAGE_COLLECTOR)
                .get()
                .getConfigs())
        .isEmpty();

    soft.assertThat(api.updateRepositoryConfig().repositoryConfig(created).update().getPrevious())
        .isNull();

    soft.assertThat(
            api.getRepositoryConfig()
                .type(RepositoryConfig.Type.GARBAGE_COLLECTOR)
                .get()
                .getConfigs())
        .containsExactly(created);

    soft.assertThat(api.updateRepositoryConfig().repositoryConfig(updated).update().getPrevious())
        .isEqualTo(created);

    soft.assertThat(
            api.getRepositoryConfig()
                .type(RepositoryConfig.Type.GARBAGE_COLLECTOR)
                .get()
                .getConfigs())
        .containsExactly(updated);
  }

  @Test
  @NessieApiVersions(versions = NessieApiVersion.V2)
  public void genericRepositoryConfigForbidden() {
    @SuppressWarnings("resource")
    NessieApiV2 api = apiV2();

    RepositoryConfig created =
        ImmutableGenericRepositoryConfig.builder()
            .type(
                new RepositoryConfig.Type() {
                  @Override
                  public String name() {
                    return "FOO_BAR";
                  }

                  @Override
                  public Class<? extends RepositoryConfig> type() {
                    return GenericRepositoryConfig.class;
                  }
                })
            .putAttributes("foo", "bar")
            .putAttributes("bar", "baz")
            .build();

    soft.assertThatThrownBy(
            () -> api.updateRepositoryConfig().repositoryConfig(created).update().getPrevious())
        .isInstanceOf(NessieBadRequestException.class)
        .hasMessageContaining(
            "Repository config type bundle for 'FOO_BAR' is not available on the Nessie server side");
  }

  @Test
  @NessieApiVersions(versions = NessieApiVersion.V2)
  public void invalidCreateRepositoryConfig() {
    @SuppressWarnings("resource")
    NessieApiV2 api = apiV2();

    api().getConfig();

    soft.assertThatThrownBy(
            () ->
                api.updateRepositoryConfig()
                    .repositoryConfig(
                        GarbageCollectorConfig.builder()
                            .defaultCutoffPolicy("foo")
                            .newFilesGracePeriod(Duration.of(3, ChronoUnit.HOURS))
                            .build())
                    .update()
                    .getPrevious())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Failed to parse default-cutoff-value");
  }

  @Test
  public void invalidParameters() {
    soft.assertThatThrownBy(() -> api().getEntries().refName("..invalid..").get())
        .isInstanceOf(NessieBadRequestException.class)
        .hasMessageContaining(Validation.REF_NAME_MESSAGE);
  }

  @Test
  @NessieApiVersions(versions = NessieApiVersion.V2)
  public void renameTwice() throws Exception {
    Branch main = api().getDefaultBranch();
    soft.assertThat(api().getAllReferences().get().getReferences()).containsExactly(main);

    ContentKey key = ContentKey.of("table");
    ContentKey keyBackup = ContentKey.of("table_backup");
    ContentKey keyTemp = ContentKey.of("table_tmp");
    List<ContentKey> keys = ImmutableList.of(key, keyTemp, keyBackup);

    IcebergTable tableOld = IcebergTable.of("old", 1, 2, 3, 4);
    IcebergTable tableNew = IcebergTable.of("new", 1, 2, 3, 4);

    CommitResponse createTable =
        prepCommit(main, "commit", Put.of(key, tableOld)).commitWithResponse();
    tableOld = createTable.contentWithAssignedId(key, tableOld);

    soft.assertThat(api().getContent().reference(createTable.getTargetBranch()).key(key).get())
        .hasSize(1)
        .containsEntry(key, tableOld);

    // create new "table_tmp"

    CommitResponse createNewTemp =
        prepCommit(createTable.getTargetBranch(), "new", Put.of(keyTemp, tableNew))
            .commitWithResponse();
    tableNew = createNewTemp.contentWithAssignedId(keyTemp, tableNew);

    soft.assertThat(api().getContent().reference(createNewTemp.getTargetBranch()).keys(keys).get())
        .hasSize(2)
        .containsEntry(key, tableOld)
        .containsEntry(keyTemp, tableNew);

    // rename "original" to "original_backup"

    CommitResponse renameToBackup =
        prepCommit(
                createNewTemp.getTargetBranch(),
                "backup",
                Delete.of(key),
                Put.of(keyBackup, tableOld))
            .commitWithResponse();

    soft.assertThat(api().getContent().reference(renameToBackup.getTargetBranch()).keys(keys).get())
        .hasSize(2)
        .containsEntry(keyBackup, tableOld)
        .containsEntry(keyTemp, tableNew);

    // rename new "table_tmp" to "table"

    CommitResponse renameNew =
        prepCommit(
                renameToBackup.getTargetBranch(),
                "rename new",
                Delete.of(keyTemp),
                Put.of(key, tableNew))
            .commitWithResponse();

    soft.assertThat(api().getContent().reference(renameNew.getTargetBranch()).keys(keys).get())
        .hasSize(2)
        .containsEntry(keyBackup, tableOld)
        .containsEntry(key, tableNew);

    // delete backup "table_backup"

    CommitResponse deleteOld =
        prepCommit(renameNew.getTargetBranch(), "delete", Delete.of(keyBackup))
            .commitWithResponse();

    soft.assertThat(api().getContent().reference(deleteOld.getTargetBranch()).keys(keys).get())
        .hasSize(1)
        .containsEntry(key, tableNew);
  }

  @Test
  @NessieApiVersions(versions = {NessieApiVersion.V2})
  public void testErrorsV2() throws Exception {
    ContentKey key = ContentKey.of("namespace", "foo");
    IcebergTable table = IcebergTable.of("content-table1", 42, 42, 42, 42);

    Branch main = api().getDefaultBranch();

    Branch branch =
        (Branch)
            api()
                .createReference()
                .reference(Branch.of("ref-conflicts", main.getHash()))
                .sourceRefName(main.getName())
                .create();

    soft.assertThatThrownBy(
            () ->
                api()
                    .commitMultipleOperations()
                    .commitMeta(fromMessage("commit"))
                    .operation(Put.of(key, table))
                    .branch(branch)
                    .commit())
        .isInstanceOf(NessieReferenceConflictException.class)
        .asInstanceOf(type(NessieReferenceConflictException.class))
        .matches(e -> e.getErrorCode().equals(ErrorCode.REFERENCE_CONFLICT))
        .extracting(NessieReferenceConflictException::getErrorDetails)
        .asInstanceOf(type(ReferenceConflicts.class))
        .extracting(ReferenceConflicts::conflicts, list(Conflict.class))
        .hasSize(1)
        .extracting(Conflict::conflictType)
        .containsExactly(ConflictType.NAMESPACE_ABSENT);

    soft.assertThatThrownBy(() -> api().getReference().refName("main@12345678").get())
        .isInstanceOf(NessieBadRequestException.class)
        .hasMessageEndingWith("Hashes are not allowed when fetching a reference by name");
  }

  @Test
  @NessieApiVersions(versions = {NessieApiVersion.V2})
  public void referenceHistory() throws Exception {
    Branch main = api().getDefaultBranch();

    Branch branch =
        (Branch)
            api()
                .createReference()
                .reference(Branch.of("ref-history", main.getHash()))
                .sourceRefName(main.getName())
                .create();

    List<String> expectedHashes = new ArrayList<>();
    for (int c = 0; c < 10; c++) {
      expectedHashes.add(branch.getHash());
      branch =
          prepCommit(branch, "commit-" + c)
              .operation(Put.of(ContentKey.of("t-" + c), IcebergTable.of("m-" + c, 1, 2, 3, 4)))
              .commit();
    }
    Collections.reverse(expectedHashes);

    // Functionality is tested in the version-store tests, this test only validates that there is a
    // result.

    @SuppressWarnings("resource")
    NessieApiV2 api = apiV2();

    ReferenceHistoryResponse response =
        api.referenceHistory().refName(branch.getName()).headCommitsToScan(1000).get();
    soft.assertThat(response)
        .extracting(
            ReferenceHistoryResponse::getReference, ReferenceHistoryResponse::commitLogConsistency)
        .containsExactly(branch, CommitConsistency.COMMIT_CONSISTENT);
    soft.assertThat(response.current())
        .extracting(ReferenceHistoryState::commitHash, ReferenceHistoryState::commitConsistency)
        .containsExactly(branch.getHash(), CommitConsistency.COMMIT_CONSISTENT);
    soft.assertThat(response.previous())
        .hasSize(10)
        .extracting(ReferenceHistoryState::commitConsistency)
        .containsOnly(CommitConsistency.COMMIT_CONSISTENT);
    soft.assertThat(response.previous())
        .hasSize(10)
        .extracting(ReferenceHistoryState::commitHash)
        .containsExactlyElementsOf(expectedHashes);
  }
}

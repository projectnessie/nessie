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

import static com.google.common.collect.Maps.immutableEntry;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.projectnessie.model.CommitMeta.fromMessage;
import static org.projectnessie.model.FetchOption.ALL;

import com.google.common.hash.Hashing;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.validation.constraints.NotNull;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.projectnessie.client.api.CommitMultipleOperationsBuilder;
import org.projectnessie.client.api.NessieApiV1;
import org.projectnessie.client.api.NessieApiV2;
import org.projectnessie.client.ext.NessieApiVersion;
import org.projectnessie.client.ext.NessieApiVersions;
import org.projectnessie.client.ext.NessieClientFactory;
import org.projectnessie.error.BaseNessieClientServerException;
import org.projectnessie.error.NessieBadRequestException;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNamespaceNotEmptyException;
import org.projectnessie.error.NessieNamespaceNotFoundException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.error.NessieReferenceConflictException;
import org.projectnessie.model.Branch;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.CommitResponse;
import org.projectnessie.model.CommitResponse.AddedContent;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.DiffResponse;
import org.projectnessie.model.DiffResponse.DiffEntry;
import org.projectnessie.model.EntriesResponse;
import org.projectnessie.model.EntriesResponse.Entry;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.IcebergView;
import org.projectnessie.model.ImmutableReferenceMetadata;
import org.projectnessie.model.LogResponse;
import org.projectnessie.model.LogResponse.LogEntry;
import org.projectnessie.model.Namespace;
import org.projectnessie.model.NessieConfiguration;
import org.projectnessie.model.Operation;
import org.projectnessie.model.Operation.Delete;
import org.projectnessie.model.Operation.Put;
import org.projectnessie.model.Reference;
import org.projectnessie.model.ReferencesResponse;
import org.projectnessie.model.Tag;

/** Nessie-API tests. */
@NessieApiVersions // all versions
@SuppressWarnings("resource")
public abstract class BaseTestNessieApi {

  public static final String EMPTY = Hashing.sha256().hashString("empty", UTF_8).toString();

  private NessieApiV1 api;

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
  }

  @NotNull
  public NessieApiV1 api() {
    return api;
  }

  public boolean isV2() {
    return api instanceof NessieApiV2;
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
    return Put.of(ContentKey.of(elements), IcebergTable.of("foo", 1, 2, 3, 4));
  }

  protected static boolean pagingSupported(ThrowingCallable o)
      throws BaseNessieClientServerException {
    try {
      o.call();
      return true;
    } catch (UnsupportedOperationException | NessieBadRequestException e) {
      if (e.getMessage().contains("not supported")) {
        return false;
      }
      throw e;
    } catch (BaseNessieClientServerException e) {
      throw e;
    } catch (Throwable e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void config() throws NessieNotFoundException {
    NessieConfiguration config = api().getConfig();
    soft.assertThat(config)
        .extracting(
            NessieConfiguration::getDefaultBranch, NessieConfiguration::getMaxSupportedApiVersion)
        .containsExactly("main", 2);

    soft.assertThat(api().getDefaultBranch())
        .extracting(Branch::getName, Branch::getHash)
        .containsExactly(config.getDefaultBranch(), EMPTY);
  }

  @Test
  public void references() throws Exception {
    Branch main = api().getDefaultBranch();
    soft.assertThat(api().getAllReferences().get().getReferences()).containsExactly(main);

    Branch main1 = prepCommit(main, "commit", dummyPut("key", "foo")).commit();

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

    if (isV2()) {
      soft.assertThatThrownBy(() -> api.assignBranch().branch(branch).assignTo(main).assignAndGet())
          .isInstanceOf(NessieReferenceConflictException.class);
    } else {
      soft.assertThatThrownBy(() -> api.assignBranch().branch(branch).assignTo(main).assign())
          .isInstanceOf(NessieReferenceConflictException.class);
    }

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

    if (isV2()) {
      soft.assertThatThrownBy(() -> api().deleteBranch().branch(branch).getAndDelete())
          .isInstanceOf(NessieReferenceConflictException.class);
    } else {
      soft.assertThatThrownBy(() -> api().deleteBranch().branch(branch).delete())
          .isInstanceOf(NessieReferenceConflictException.class);
    }

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

    Reference branch2 = createReference(Branch.of("branch2", null), main.getName());
    soft.assertThat(branch2).isEqualTo(Branch.of("branch2", EMPTY));

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

  @Test
  public void commitMergeTransplant() throws Exception {
    Branch main = api().getDefaultBranch();

    main = prepCommit(main, "common ancestor", dummyPut("unrelated")).commit();
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
    } else {
      branch = prepCommit(branch, "one", dummyPut("a", "a")).commit();
      branch = prepCommit(branch, "two", dummyPut("b", "a"), dummyPut("b", "b")).commit();
    }

    soft.assertThat(api().getCommitLog().refName(branch.getName()).get().getLogEntries())
        .hasSize(4);

    soft.assertThat(api().getEntries().reference(branch).get().getEntries())
        .extracting(EntriesResponse.Entry::getName)
        .containsExactlyInAnyOrder(
            ContentKey.of("a", "a"), ContentKey.of("b", "a"), ContentKey.of("b", "b"));

    soft.assertThat(api().getCommitLog().refName(main.getName()).get().getLogEntries()).hasSize(2);
    soft.assertThat(api().getEntries().reference(main).get().getEntries())
        .extracting(EntriesResponse.Entry::getName)
        .isEmpty();

    api().mergeRefIntoBranch().fromRef(branch).branch(main).keepIndividualCommits(false).merge();
    Reference main2 = api().getReference().refName(main.getName()).get();
    soft.assertThat(api().getCommitLog().refName(main.getName()).get().getLogEntries()).hasSize(3);

    soft.assertThat(api().getEntries().reference(main2).get().getEntries())
        .extracting(EntriesResponse.Entry::getName)
        .containsExactlyInAnyOrder(
            ContentKey.of("a", "a"), ContentKey.of("b", "a"), ContentKey.of("b", "b"));

    soft.assertThat(api().getEntries().reference(otherBranch).get().getEntries()).isEmpty();
    api()
        .transplantCommitsIntoBranch()
        .fromRefName(main.getName())
        .hashesToTransplant(singletonList(main2.getHash()))
        .branch(otherBranch)
        .transplant();
    soft.assertThat(api().getEntries().refName(otherBranch.getName()).get().getEntries())
        .extracting(EntriesResponse.Entry::getName)
        .containsExactlyInAnyOrder(
            ContentKey.of("a", "a"), ContentKey.of("b", "a"), ContentKey.of("b", "b"));

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

  @Test
  public void diff() throws Exception {
    Branch main = api().getDefaultBranch();
    Branch branch1 = createReference(Branch.of("b1", main.getHash()), main.getName());
    Branch branch2 = createReference(Branch.of("b2", main.getHash()), main.getName());

    branch1 =
        prepCommit(branch1, "c1", dummyPut("1", "1"), dummyPut("1", "2"), dummyPut("1", "3"))
            .commit();
    branch2 =
        prepCommit(branch2, "c2", dummyPut("1", "1"), dummyPut("3", "1"), dummyPut("4", "1"))
            .commit();

    ContentKey key11 = ContentKey.of("1", "1");
    ContentKey key12 = ContentKey.of("1", "2");
    ContentKey key13 = ContentKey.of("1", "3");
    ContentKey key31 = ContentKey.of("3", "1");
    ContentKey key41 = ContentKey.of("4", "1");
    Map<ContentKey, Content> contents1 =
        api().getContent().reference(branch1).key(key11).key(key12).key(key13).get();
    Map<ContentKey, Content> contents2 =
        api().getContent().reference(branch2).key(key11).key(key31).key(key41).get();

    List<DiffEntry> diff1 = api().getDiff().fromRef(branch1).toRef(branch2).get().getDiffs();

    soft.assertThat(diff1)
        .containsExactlyInAnyOrder(
            DiffEntry.diffEntry(key11, contents1.get(key11), contents2.get(key11)),
            DiffEntry.diffEntry(key12, contents1.get(key12), null),
            DiffEntry.diffEntry(key13, contents1.get(key13), null),
            DiffEntry.diffEntry(key31, null, contents2.get(key31)),
            DiffEntry.diffEntry(key41, null, contents2.get(key41)));

    List<DiffEntry> diff2 =
        api().getDiff().fromRefName(branch1.getName()).toRef(branch2).get().getDiffs();
    List<DiffEntry> diff3 =
        api().getDiff().fromRef(branch1).toRefName(branch2.getName()).get().getDiffs();
    soft.assertThat(diff1).isEqualTo(diff2).isEqualTo(diff3);

    if (pagingSupported(
        () -> api().getDiff().fromRef(main).toRef(main).pageToken("666f6f").get())) {

      // Paging

      List<DiffEntry> all = new ArrayList<>();
      String token = null;
      for (int i = 0; i < 5; i++) {
        DiffResponse resp =
            api().getDiff().fromRef(branch1).toRef(branch2).maxRecords(1).pageToken(token).get();
        all.addAll(resp.getDiffs());
        token = resp.getToken();
        if (i == 4) {
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
      main = prepCommit(main, "c-" + i, dummyPut("c", Integer.toString(i))).commit();
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

    if (pagingSupported(() -> api().getAllReferences().pageToken("666f6f").get())) {
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
  }

  @Test
  public void entries() throws Exception {
    Branch main0 = api().getDefaultBranch();

    Branch main =
        prepCommit(
                main0,
                "commit",
                IntStream.range(0, 10)
                    .mapToObj(i -> dummyPut("c", Integer.toString(i)))
                    .toArray(Operation[]::new))
            .commit();

    List<EntriesResponse.Entry> notPaged = api().getEntries().reference(main).get().getEntries();
    soft.assertThat(notPaged).hasSize(10);

    if (pagingSupported(() -> api().getEntries().reference(main0).pageToken("666f6f").get())) {
      List<EntriesResponse.Entry> all = new ArrayList<>();
      String token = null;
      for (int i = 0; i < 10; i++) {
        EntriesResponse resp =
            api().getEntries().reference(main).maxRecords(1).pageToken(token).get();
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

      soft.assertThat(api().getEntries().reference(main).maxRecords(1).stream())
          .containsExactlyInAnyOrderElementsOf(all);
    }
  }

  @Test
  public void namespaces() throws Exception {
    Branch main = api().getDefaultBranch();

    soft.assertThat(
            api()
                .getMultipleNamespaces()
                .reference(main)
                .namespace(Namespace.EMPTY)
                .get()
                .getNamespaces())
        .isEmpty();

    Namespace namespace1 = Namespace.of("a");
    Namespace namespace2 = Namespace.of("a", "b.b");
    Namespace namespace3 = Namespace.of("a", "b.bbbb");
    Namespace namespace4 = Namespace.of("a", "b.b", "c");
    Namespace namespace1WithId =
        api().createNamespace().refName(main.getName()).namespace(namespace1).create();
    Namespace namespace2WithId =
        api().createNamespace().refName(main.getName()).namespace(namespace2).create();
    Namespace namespace3WithId =
        api().createNamespace().refName(main.getName()).namespace(namespace3).create();
    Namespace namespace4WithId =
        api().createNamespace().refName(main.getName()).namespace(namespace4).create();

    soft.assertThat(
            api()
                .getMultipleNamespaces()
                .refName(main.getName())
                .namespace(Namespace.EMPTY)
                .get()
                .getNamespaces())
        .containsExactlyInAnyOrder(
            namespace1WithId, namespace2WithId, namespace3WithId, namespace4WithId);
    soft.assertThat(
            api()
                .getMultipleNamespaces()
                .refName(main.getName())
                .namespace(namespace1)
                .get()
                .getNamespaces())
        .containsExactlyInAnyOrder(
            namespace1WithId, namespace2WithId, namespace3WithId, namespace4WithId);
    soft.assertThat(
            api()
                .getMultipleNamespaces()
                .refName(main.getName())
                .namespace(namespace2)
                .get()
                .getNamespaces())
        .containsExactlyInAnyOrder(namespace2WithId, namespace4WithId);

    soft.assertThat(
            api()
                .getContent()
                .refName(main.getName())
                .key(ContentKey.of(namespace1.getElements()))
                .key(ContentKey.of(namespace2.getElements()))
                .key(ContentKey.of(namespace3.getElements()))
                .key(ContentKey.of(namespace4.getElements()))
                .get())
        .containsEntry(ContentKey.of(namespace1.getElements()), namespace1WithId)
        .containsEntry(ContentKey.of(namespace2.getElements()), namespace2WithId)
        .containsEntry(ContentKey.of(namespace3.getElements()), namespace3WithId)
        .containsEntry(ContentKey.of(namespace4.getElements()), namespace4WithId);

    soft.assertThat(api().getNamespace().refName(main.getName()).namespace(namespace1).get())
        .isEqualTo(namespace1WithId);
    soft.assertThat(api().getNamespace().refName(main.getName()).namespace(namespace2).get())
        .isEqualTo(namespace2WithId);
    soft.assertThat(api().getNamespace().refName(main.getName()).namespace(namespace3).get())
        .isEqualTo(namespace3WithId);
    soft.assertThat(api().getNamespace().refName(main.getName()).namespace(namespace4).get())
        .isEqualTo(namespace4WithId);

    api()
        .updateProperties()
        .refName(main.getName())
        .namespace(namespace2)
        .updateProperty("foo", "bar")
        .updateProperty("bar", "baz")
        .update();
    Namespace namespace2update =
        (Namespace)
            api()
                .getContent()
                .refName(main.getName())
                .key(ContentKey.of("a", "b.b"))
                .get()
                .get(ContentKey.of("a", "b.b"));
    soft.assertThat(api().getNamespace().refName(main.getName()).namespace(namespace2).get())
        .isEqualTo(namespace2update);

    soft.assertThat(
            api()
                .getMultipleNamespaces()
                .refName(main.getName())
                .namespace(Namespace.EMPTY)
                .get()
                .getNamespaces())
        .containsExactlyInAnyOrder(
            namespace1WithId, namespace2update, namespace3WithId, namespace4WithId);

    api()
        .updateProperties()
        .refName(main.getName())
        .namespace(namespace2)
        .removeProperty("foo")
        .update();
    Namespace namespace2update2 =
        (Namespace)
            api()
                .getContent()
                .refName(main.getName())
                .key(ContentKey.of("a", "b.b"))
                .get()
                .get(ContentKey.of("a", "b.b"));
    soft.assertThat(api().getNamespace().refName(main.getName()).namespace(namespace2).get())
        .isEqualTo(namespace2update2);

    soft.assertThat(
            api()
                .getMultipleNamespaces()
                .refName(main.getName())
                .namespace(Namespace.EMPTY)
                .get()
                .getNamespaces())
        .containsExactlyInAnyOrder(
            namespace1WithId, namespace2update2, namespace3WithId, namespace4WithId);

    if (isV2()) {
      soft.assertThatThrownBy(
              () -> api().deleteNamespace().refName(main.getName()).namespace(namespace2).delete())
          .isInstanceOf(NessieNamespaceNotEmptyException.class);
    }

    api().deleteNamespace().refName(main.getName()).namespace(namespace4).delete();

    soft.assertThat(
            api().getContent().refName(main.getName()).key(ContentKey.of("a", "b.b", "c")).get())
        .isEmpty();

    soft.assertThatThrownBy(
            () -> api().getNamespace().refName(main.getName()).namespace(namespace4).get())
        .isInstanceOf(NessieNamespaceNotFoundException.class);

    soft.assertThatThrownBy(
            () -> api().deleteNamespace().refName(main.getName()).namespace(namespace4).delete())
        .isInstanceOf(NessieNamespaceNotFoundException.class);

    soft.assertThat(
            api()
                .getMultipleNamespaces()
                .refName(main.getName())
                .namespace(Namespace.EMPTY)
                .get()
                .getNamespaces())
        .containsExactlyInAnyOrder(namespace1WithId, namespace2update2, namespace3WithId);

    // This one fails, if the namespace-path 'startswith' filter (REST v2) to check for child
    // content is incorrectly implemented.
    soft.assertThatCode(
            () -> api().deleteNamespace().refName(main.getName()).namespace(namespace2).delete())
        .doesNotThrowAnyException();
  }

  @Test
  @NessieApiVersions(versions = NessieApiVersion.V2)
  public void commitLogForNamelessReference() throws BaseNessieClientServerException {
    Branch main = api().getDefaultBranch();
    Branch branch =
        createReference(Branch.of("commitLogForNamelessReference", main.getHash()), main.getName());
    for (int i = 0; i < 5; i++) {
      branch = prepCommit(branch, "c-" + i, dummyPut("c", Integer.toString(i))).commit();
    }
    List<LogResponse.LogEntry> log =
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
    IcebergView tb = IcebergView.of("pathx", 1, 1, "select * from table", "Dremio");
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
}

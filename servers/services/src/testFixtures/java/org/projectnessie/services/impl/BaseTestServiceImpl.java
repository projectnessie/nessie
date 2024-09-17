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
package org.projectnessie.services.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.projectnessie.model.CommitMeta.fromMessage;
import static org.projectnessie.model.FetchOption.MINIMAL;
import static org.projectnessie.model.Reference.ReferenceType.BRANCH;
import static org.projectnessie.model.Reference.ReferenceType.TAG;
import static org.projectnessie.services.authz.ApiContext.apiContext;
import static org.projectnessie.services.impl.RefUtil.toReference;
import static org.projectnessie.versioned.RequestMeta.API_READ;
import static org.projectnessie.versioned.RequestMeta.API_WRITE;
import static org.projectnessie.versioned.storage.common.logic.Logics.repositoryLogic;

import com.google.common.collect.ImmutableMap;
import java.security.Principal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieContentNotFoundException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Branch;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.CommitResponse;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.ContentResponse;
import org.projectnessie.model.Detached;
import org.projectnessie.model.DiffResponse.DiffEntry;
import org.projectnessie.model.EntriesResponse.Entry;
import org.projectnessie.model.FetchOption;
import org.projectnessie.model.GetMultipleContentsResponse.ContentWithKey;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.ImmutableOperations;
import org.projectnessie.model.LogResponse.LogEntry;
import org.projectnessie.model.Namespace;
import org.projectnessie.model.Operation;
import org.projectnessie.model.Operation.Put;
import org.projectnessie.model.Operations;
import org.projectnessie.model.Reference;
import org.projectnessie.model.Tag;
import org.projectnessie.services.authz.AbstractBatchAccessChecker;
import org.projectnessie.services.authz.AccessContext;
import org.projectnessie.services.authz.Authorizer;
import org.projectnessie.services.authz.BatchAccessChecker;
import org.projectnessie.services.config.ServerConfig;
import org.projectnessie.services.spi.PagedResponseHandler;
import org.projectnessie.versioned.VersionStore;
import org.projectnessie.versioned.storage.common.persist.Persist;
import org.projectnessie.versioned.storage.testextension.NessiePersist;
import org.projectnessie.versioned.storage.versionstore.VersionStoreImpl;

@ExtendWith(SoftAssertionsExtension.class)
public abstract class BaseTestServiceImpl {

  @NessiePersist(initializeRepo = false)
  Persist persist;

  protected static final ServerConfig DEFAULT_SERVER_CONFIG =
      new ServerConfig() {
        @Override
        public String getDefaultBranch() {
          return "defaultBranch";
        }

        @Override
        public boolean sendStacktraceToClient() {
          return true;
        }
      };

  protected static final Authorizer NOOP_AUTHORIZER =
      (context, apiContext) -> AbstractBatchAccessChecker.NOOP_ACCESS_CHECKER;

  @InjectSoftAssertions protected SoftAssertions soft;

  private Authorizer authorizer = NOOP_AUTHORIZER;
  private Principal principal;

  protected final ConfigApiImpl configApi() {
    return new ConfigApiImpl(
        config(), versionStore(), authorizer(), this::principal, apiContext("Nessie", 2));
  }

  protected final TreeApiImpl treeApi() {
    return new TreeApiImpl(
        config(), versionStore(), authorizer(), this::principal, apiContext("Nessie", 2));
  }

  protected final ContentApiImpl contentApi() {
    return new ContentApiImpl(
        config(), versionStore(), authorizer(), this::principal, apiContext("Nessie", 2));
  }

  protected final DiffApiImpl diffApi() {
    return new DiffApiImpl(
        config(), versionStore(), authorizer(), this::principal, apiContext("Nessie", 2));
  }

  protected final NamespaceApiImpl namespaceApi() {
    return new NamespaceApiImpl(
        config(), versionStore(), authorizer(), this::principal, apiContext("Nessie", 2));
  }

  protected Principal principal() {
    return principal;
  }

  protected void setPrincipal(Principal principal) {
    this.principal = principal;
  }

  protected Authorizer authorizer() {
    return authorizer;
  }

  protected void setAuthorizer(Authorizer authorizer) {
    this.authorizer = authorizer;
  }

  protected void setBatchAccessChecker(
      Function<AccessContext, BatchAccessChecker> batchAccessChecker) {
    this.authorizer = (t, apiContext) -> batchAccessChecker.apply(t);
  }

  protected VersionStore versionStore() {
    return new VersionStoreImpl(persist);
  }

  @BeforeEach
  protected void setup() {
    repositoryLogic(persist).initialize(config().getDefaultBranch());
  }

  @AfterEach
  protected void cleanup() {
    authorizer = NOOP_AUTHORIZER;
    principal = null;
    if (persist != null) {
      persist.erase();
    }
  }

  protected ServerConfig config() {
    return DEFAULT_SERVER_CONFIG;
  }

  protected List<LogEntry> commitLog(String refName) throws NessieNotFoundException {
    return commitLog(refName, MINIMAL, null);
  }

  protected List<LogEntry> commitLog(String refName, FetchOption fetchOption, String filter)
      throws NessieNotFoundException {
    return commitLog(refName, fetchOption, null, null, filter);
  }

  protected List<LogEntry> commitLog(
      String refName,
      FetchOption fetchOption,
      String oldestHashLimit,
      String youngestHash,
      String filter)
      throws NessieNotFoundException {
    return treeApi()
        .getCommitLog(
            refName,
            fetchOption,
            oldestHashLimit,
            youngestHash,
            filter,
            null,
            new UnlimitedListResponseHandler<>());
  }

  protected List<Reference> allReferences() {
    return allReferences(MINIMAL, null);
  }

  protected List<Reference> allReferences(FetchOption fetchOption, String filter) {
    return treeApi()
        .getAllReferences(fetchOption, filter, null, new UnlimitedListResponseHandler<>());
  }

  protected List<Entry> withoutNamespaces(List<Entry> entries) {
    return entries.stream()
        .filter(e -> e.getType() != Content.Type.NAMESPACE)
        .collect(Collectors.toList());
  }

  protected List<Entry> entries(Reference reference) throws NessieNotFoundException {
    return entries(reference.getName(), reference.getHash(), null, null, false);
  }

  protected List<Entry> entries(String refName, String hashOnRef) throws NessieNotFoundException {
    return entries(refName, hashOnRef, null, null, false);
  }

  protected List<Entry> entries(Reference reference, Integer namespaceDepth, String filter)
      throws NessieNotFoundException {
    return entries(reference.getName(), reference.getHash(), namespaceDepth, filter, false);
  }

  protected List<Entry> entries(
      String refName, String hashOnRef, Integer namespaceDepth, String filter, boolean withContent)
      throws NessieNotFoundException {
    return treeApi()
        .getEntries(
            refName,
            hashOnRef,
            namespaceDepth,
            filter,
            null,
            withContent,
            new UnlimitedListResponseHandler<>(),
            h -> {},
            null,
            null,
            null,
            null);
  }

  protected List<DiffEntry> diff(Reference fromRef, Reference toRef)
      throws NessieNotFoundException {
    return diff(fromRef.getName(), fromRef.getHash(), toRef.getName(), toRef.getHash());
  }

  protected List<DiffEntry> diff(String fromRef, String fromHash, String toRef, String toHash)
      throws NessieNotFoundException {
    return diffApi()
        .getDiff(
            fromRef,
            fromHash,
            toRef,
            toHash,
            null,
            new UnlimitedListResponseHandler<>(),
            h -> {},
            h -> {},
            null,
            null,
            null,
            null,
            null);
  }

  protected List<LogEntry> pagedCommitLog(
      String refName, FetchOption fetchOption, String filter, int pageSize, int totalCommits)
      throws NessieNotFoundException {

    List<LogEntry> completeLog = new ArrayList<>();
    String token = null;
    for (int i = 0; ; i++) {
      AtomicReference<String> nextToken = new AtomicReference<>();
      List<LogEntry> page =
          treeApi()
              .getCommitLog(
                  refName,
                  fetchOption,
                  null,
                  null,
                  filter,
                  token,
                  new DirectPagedCountingResponseHandler<>(pageSize, nextToken::set));
      completeLog.addAll(page);
      if (nextToken.get() == null) {
        break;
      }
      token = nextToken.get();
      soft.assertThat(i).isLessThanOrEqualTo((totalCommits / pageSize) + 1);
      soft.assertAll();
    }

    return completeLog;
  }

  protected List<Reference> pagedReferences(
      FetchOption fetchOption, String filter, int pageSize, int totalCommits) {

    List<Reference> completeLog = new ArrayList<>();
    String token = null;
    for (int i = 0; ; i++) {
      AtomicReference<String> nextToken = new AtomicReference<>();
      List<Reference> page =
          treeApi()
              .getAllReferences(
                  fetchOption,
                  filter,
                  token,
                  new DirectPagedCountingResponseHandler<>(pageSize, nextToken::set));
      completeLog.addAll(page);
      if (nextToken.get() == null) {
        break;
      }
      token = nextToken.get();
      soft.assertThat(i).isLessThanOrEqualTo((totalCommits / pageSize) + 1);
      soft.assertAll();
    }

    return completeLog;
  }

  protected List<Entry> pagedEntries(
      Reference ref,
      String filter,
      int pageSize,
      int totalEntries,
      Consumer<Reference> effectiveReference,
      boolean withContent)
      throws NessieNotFoundException {

    List<Entry> completeLog = new ArrayList<>();
    String token = null;
    for (int i = 0; ; i++) {
      AtomicReference<String> nextToken = new AtomicReference<>();
      List<Entry> page =
          treeApi()
              .getEntries(
                  ref.getName(),
                  ref.getHash(),
                  null,
                  filter,
                  token,
                  withContent,
                  new DirectPagedCountingResponseHandler<>(pageSize, nextToken::set),
                  h -> effectiveReference.accept(toReference(h)),
                  null,
                  null,
                  null,
                  null);
      completeLog.addAll(page);
      if (nextToken.get() == null) {
        break;
      }
      token = nextToken.get();
      soft.assertThat(i).isLessThanOrEqualTo((totalEntries / pageSize) + 1);
      soft.assertAll();
    }

    return completeLog;
  }

  protected List<DiffEntry> pagedDiff(
      Reference fromRef,
      Reference toRef,
      int pageSize,
      int totalEntries,
      Consumer<Reference> effectiveFrom,
      Consumer<Reference> effectiveTo)
      throws NessieNotFoundException {

    List<DiffEntry> completeLog = new ArrayList<>();
    String token = null;
    for (int i = 0; ; i++) {
      AtomicReference<String> nextToken = new AtomicReference<>();
      List<DiffEntry> page =
          diffApi()
              .getDiff(
                  fromRef.getName(),
                  fromRef.getHash(),
                  toRef.getName(),
                  toRef.getHash(),
                  token,
                  new DirectPagedCountingResponseHandler<>(pageSize, nextToken::set),
                  h -> effectiveFrom.accept(toReference(h)),
                  h -> effectiveTo.accept(toReference(h)),
                  null,
                  null,
                  null,
                  null,
                  null);
      completeLog.addAll(page);
      if (nextToken.get() == null) {
        break;
      }
      token = nextToken.get();
      soft.assertThat(i).isLessThanOrEqualTo((totalEntries / pageSize) + 1);
      soft.assertAll();
    }

    return completeLog;
  }

  protected Branch createBranch(String branchName)
      throws NessieNotFoundException, NessieConflictException {
    return createBranch(branchName, treeApi().getDefaultBranch());
  }

  protected Branch createBranch(String branchName, Reference base)
      throws NessieNotFoundException, NessieConflictException {
    return (Branch) treeApi().createReference(branchName, BRANCH, base.getHash(), base.getName());
  }

  protected Tag createTag(String tagName, Reference base)
      throws NessieNotFoundException, NessieConflictException {
    return (Tag) treeApi().createReference(tagName, TAG, base.getHash(), base.getName());
  }

  protected Reference getReference(String refName) throws NessieNotFoundException {
    return treeApi().getReferenceByName(refName, MINIMAL);
  }

  protected Branch ensureNamespacesForKeysExist(Branch targetBranch, ContentKey... keysToCheck)
      throws NessieConflictException, NessieNotFoundException {
    Set<ContentKey> existingKeys =
        entries(targetBranch).stream().map(Entry::getName).collect(Collectors.toSet());

    Put[] nsToCreate =
        Arrays.stream(keysToCheck)
            .filter(k -> k.getElementCount() > 1)
            .flatMap(
                key ->
                    IntStream.rangeClosed(1, key.getElementCount() - 1)
                        .mapToObj(l -> ContentKey.of(key.getElements().subList(0, l))))
            .distinct()
            .filter(nsKey -> !existingKeys.contains(nsKey))
            .map(nsKey -> Put.of(nsKey, Namespace.of(nsKey.getElements())))
            .toArray(Put[]::new);
    if (nsToCreate.length == 0) {
      return targetBranch;
    }
    return commit(targetBranch, fromMessage("create namespaces"), nsToCreate).getTargetBranch();
  }

  protected CommitResponse commit(Branch targetBranch, CommitMeta meta, Operation... operations)
      throws NessieConflictException, NessieNotFoundException {
    return commit(targetBranch.getName(), targetBranch.getHash(), meta, operations);
  }

  protected CommitResponse commit(
      String branch, String expectedHash, CommitMeta meta, Operation... operations)
      throws NessieConflictException, NessieNotFoundException {
    Operations ops =
        ImmutableOperations.builder().addOperations(operations).commitMeta(meta).build();
    return treeApi().commitMultipleOperations(branch, expectedHash, ops, API_WRITE);
  }

  protected Map<ContentKey, Content> contents(Reference reference, ContentKey... keys)
      throws NessieNotFoundException {
    return contents(reference, false, keys);
  }

  protected Map<ContentKey, Content> contents(
      Reference reference, boolean forWrite, ContentKey... keys) throws NessieNotFoundException {
    return contents(reference.getName(), reference.getHash(), forWrite, keys);
  }

  protected Map<ContentKey, Content> contents(String refName, String hashOnRef, ContentKey... keys)
      throws NessieNotFoundException {
    return contents(refName, hashOnRef, false, keys);
  }

  protected Map<ContentKey, Content> contents(
      String refName, String hashOnRef, boolean forWrite, ContentKey... keys)
      throws NessieNotFoundException {
    return contentApi()
        .getMultipleContents(
            refName, hashOnRef, Arrays.asList(keys), false, forWrite ? API_WRITE : API_READ)
        .getContents()
        .stream()
        .collect(Collectors.toMap(ContentWithKey::getKey, ContentWithKey::getContent));
  }

  protected ContentResponse content(Reference reference, boolean forWrite, ContentKey key)
      throws NessieNotFoundException {
    return content(reference.getName(), reference.getHash(), forWrite, key);
  }

  protected ContentResponse content(
      String refName, String hashOnRef, boolean forWrite, ContentKey key)
      throws NessieNotFoundException {
    return contentApi().getContent(key, refName, hashOnRef, false, forWrite ? API_WRITE : API_READ);
  }

  protected String createCommits(
      Reference branch, int numAuthors, int commitsPerAuthor, String currentHash)
      throws NessieConflictException, NessieNotFoundException {
    for (int j = 0; j < numAuthors; j++) {
      String author = "author-" + j;
      for (int i = 0; i < commitsPerAuthor; i++) {
        ContentKey key = ContentKey.of("table" + i);

        Put op;
        try {
          Content existing =
              contentApi()
                  .getContent(key, branch.getName(), currentHash, false, API_READ)
                  .getContent();
          op = Put.of(key, IcebergTable.of("some-file-" + i, 42, 42, 42, 42, existing.getId()));
        } catch (NessieContentNotFoundException notFound) {
          op = Put.of(key, IcebergTable.of("some-file-" + i, 42, 42, 42, 42));
        }

        String nextHash =
            commit(
                    branch.getName(),
                    currentHash,
                    CommitMeta.builder()
                        .author(author)
                        .message("committed-by-" + author)
                        .properties(ImmutableMap.of("prop1", "val1", "prop2", "val2"))
                        .build(),
                    op)
                .getTargetBranch()
                .getHash();

        assertThat(currentHash).isNotEqualTo(nextHash);
        currentHash = nextHash;
      }
    }
    return currentHash;
  }

  protected static final class UnlimitedListResponseHandler<E>
      implements PagedResponseHandler<List<E>, E> {

    final List<E> result = new ArrayList<>();

    @Override
    public List<E> build() {
      return result;
    }

    @Override
    public boolean addEntry(E entry) {
      result.add(entry);
      return true;
    }

    @Override
    public void hasMore(String pagingToken) {
      throw new UnsupportedOperationException();
    }
  }

  protected static List<Operation> operationsWithoutContentId(List<Operation> operations) {
    if (operations == null) {
      return null;
    }
    return operations.stream()
        .map(BaseTestServiceImpl::operationWithoutContentId)
        .collect(Collectors.toList());
  }

  protected static Operation operationWithoutContentId(Operation op) {
    if (op instanceof Put) {
      Put put = (Put) op;
      return Put.of(put.getKey(), contentWithoutId(put.getContent()));
    }
    return op;
  }

  protected static Content contentWithoutId(Content content) {
    if (content == null) {
      return null;
    }
    return content.withId(null);
  }

  protected static DiffEntry diffEntryWithoutContentId(DiffEntry diff) {
    if (diff == null) {
      return null;
    }
    return DiffEntry.diffEntry(
        diff.getKey(), contentWithoutId(diff.getFrom()), contentWithoutId(diff.getTo()));
  }

  protected static String maybeAsDetachedName(boolean withDetachedCommit, Reference ref) {
    return withDetachedCommit ? Detached.REF_NAME : ref.getName();
  }
}

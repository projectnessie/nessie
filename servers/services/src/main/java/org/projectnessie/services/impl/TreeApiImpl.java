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
package org.projectnessie.services.impl;

import static org.projectnessie.services.cel.CELUtil.COMMIT_LOG_DECLARATIONS;
import static org.projectnessie.services.cel.CELUtil.COMMIT_LOG_TYPES;
import static org.projectnessie.services.cel.CELUtil.CONTAINER;
import static org.projectnessie.services.cel.CELUtil.ENTRIES_DECLARATIONS;
import static org.projectnessie.services.cel.CELUtil.REFERENCES_DECLARATIONS;
import static org.projectnessie.services.cel.CELUtil.REFERENCES_TYPES;
import static org.projectnessie.services.cel.CELUtil.SCRIPT_HOST;
import static org.projectnessie.services.cel.CELUtil.VAR_COMMIT;
import static org.projectnessie.services.cel.CELUtil.VAR_CONTENT_TYPE;
import static org.projectnessie.services.cel.CELUtil.VAR_ENTRY;
import static org.projectnessie.services.cel.CELUtil.VAR_NAMESPACE;
import static org.projectnessie.services.cel.CELUtil.VAR_OPERATIONS;
import static org.projectnessie.services.cel.CELUtil.VAR_REF;
import static org.projectnessie.services.cel.CELUtil.VAR_REF_META;
import static org.projectnessie.services.cel.CELUtil.VAR_REF_TYPE;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.security.Principal;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import javax.annotation.Nullable;
import org.projectnessie.api.TreeApi;
import org.projectnessie.api.params.CommitLogParams;
import org.projectnessie.api.params.EntriesParams;
import org.projectnessie.api.params.FetchOption;
import org.projectnessie.api.params.GetReferenceParams;
import org.projectnessie.api.params.ReferencesParams;
import org.projectnessie.cel.tools.Script;
import org.projectnessie.cel.tools.ScriptException;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.error.NessieReferenceAlreadyExistsException;
import org.projectnessie.error.NessieReferenceConflictException;
import org.projectnessie.error.NessieReferenceNotFoundException;
import org.projectnessie.model.Branch;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.Content;
import org.projectnessie.model.Content.Type;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.EntriesResponse;
import org.projectnessie.model.ImmutableBranch;
import org.projectnessie.model.ImmutableLogEntry;
import org.projectnessie.model.ImmutableLogResponse;
import org.projectnessie.model.ImmutableReferenceMetadata;
import org.projectnessie.model.ImmutableReferencesResponse;
import org.projectnessie.model.ImmutableTag;
import org.projectnessie.model.LogResponse;
import org.projectnessie.model.LogResponse.LogEntry;
import org.projectnessie.model.Merge;
import org.projectnessie.model.Operation;
import org.projectnessie.model.Operations;
import org.projectnessie.model.Reference;
import org.projectnessie.model.ReferenceMetadata;
import org.projectnessie.model.ReferencesResponse;
import org.projectnessie.model.Tag;
import org.projectnessie.model.Transplant;
import org.projectnessie.services.authz.AccessChecker;
import org.projectnessie.services.cel.CELUtil;
import org.projectnessie.services.config.ServerConfig;
import org.projectnessie.versioned.BranchName;
import org.projectnessie.versioned.Commit;
import org.projectnessie.versioned.Delete;
import org.projectnessie.versioned.GetNamedRefsParams;
import org.projectnessie.versioned.GetNamedRefsParams.RetrieveOptions;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.Key;
import org.projectnessie.versioned.NamedRef;
import org.projectnessie.versioned.Put;
import org.projectnessie.versioned.Ref;
import org.projectnessie.versioned.ReferenceAlreadyExistsException;
import org.projectnessie.versioned.ReferenceConflictException;
import org.projectnessie.versioned.ReferenceInfo;
import org.projectnessie.versioned.ReferenceNotFoundException;
import org.projectnessie.versioned.TagName;
import org.projectnessie.versioned.Unchanged;
import org.projectnessie.versioned.VersionStore;
import org.projectnessie.versioned.WithHash;

public class TreeApiImpl extends BaseApiImpl implements TreeApi {

  private static final int MAX_COMMIT_LOG_ENTRIES = 250;

  public TreeApiImpl(
      ServerConfig config,
      VersionStore<Content, CommitMeta, Content.Type> store,
      AccessChecker accessChecker,
      Principal principal) {
    super(config, store, accessChecker, principal);
  }

  @Override
  public ReferencesResponse getAllReferences(ReferencesParams params) {
    Preconditions.checkArgument(params.pageToken() == null, "Paging not supported");
    ImmutableReferencesResponse.Builder resp = ReferencesResponse.builder();
    boolean fetchAll = FetchOption.isFetchAll(params.fetchOption());
    try (Stream<ReferenceInfo<CommitMeta>> str =
        getStore().getNamedRefs(getGetNamedRefsParams(fetchAll))) {
      Stream<Reference> unfiltered =
          str.map(refInfo -> TreeApiImpl.makeReference(refInfo, fetchAll));
      Stream<Reference> filtered = filterReferences(unfiltered, params.filter());
      filtered.forEach(resp::addReferences);
    } catch (ReferenceNotFoundException e) {
      throw new IllegalArgumentException(
          String.format(
              "Could not find default branch '%s'.", this.getConfig().getDefaultBranch()));
    }
    return resp.build();
  }

  private GetNamedRefsParams getGetNamedRefsParams(boolean fetchMetadata) {
    return fetchMetadata
        ? GetNamedRefsParams.builder()
            .baseReference(BranchName.of(this.getConfig().getDefaultBranch()))
            .branchRetrieveOptions(RetrieveOptions.BASE_REFERENCE_RELATED_AND_COMMIT_META)
            .tagRetrieveOptions(RetrieveOptions.COMMIT_META)
            .build()
        : GetNamedRefsParams.DEFAULT;
  }

  /**
   * Applies different filters to the {@link Stream} of references on the filter.
   *
   * @param references The references that different filters will be applied to
   * @param filter The filter to filter by
   * @return A potentially filtered {@link Stream} of commits based on the filter
   */
  private Stream<Reference> filterReferences(Stream<Reference> references, String filter) {
    if (Strings.isNullOrEmpty(filter)) {
      return references;
    }

    final Script script;
    try {
      script =
          SCRIPT_HOST
              .buildScript(filter)
              .withContainer(CONTAINER)
              .withDeclarations(REFERENCES_DECLARATIONS)
              .withTypes(REFERENCES_TYPES)
              .build();
    } catch (ScriptException e) {
      throw new IllegalArgumentException(e);
    }
    return references.filter(
        reference -> {
          try {
            ReferenceMetadata refMeta = reference.getMetadata();
            if (refMeta == null) {
              refMeta = CELUtil.EMPTY_REFERENCE_METADATA;
            }
            CommitMeta commit = refMeta.getCommitMetaOfHEAD();
            if (commit == null) {
              commit = CELUtil.EMPTY_COMMIT_META;
            }
            String refType;
            if (reference instanceof Branch) {
              refType = "BRANCH";
            } else if (reference instanceof Tag) {
              refType = "TAG";
            } else {
              refType = "REFERENCE";
            }
            return script.execute(
                Boolean.class,
                ImmutableMap.of(
                    VAR_REF,
                    reference,
                    VAR_REF_TYPE,
                    refType,
                    VAR_COMMIT,
                    commit,
                    VAR_REF_META,
                    refMeta));
          } catch (ScriptException e) {
            throw new RuntimeException(e);
          }
        });
  }

  @Override
  public Reference getReferenceByName(GetReferenceParams params) throws NessieNotFoundException {
    try {
      boolean fetchAll = FetchOption.isFetchAll(params.fetchOption());
      return makeReference(
          getStore().getNamedRef(params.getRefName(), getGetNamedRefsParams(fetchAll)), fetchAll);
    } catch (ReferenceNotFoundException e) {
      throw new NessieReferenceNotFoundException(e.getMessage(), e);
    }
  }

  @Override
  public Reference createReference(String sourceRefName, Reference reference)
      throws NessieNotFoundException, NessieConflictException {
    NamedRef namedReference;
    if (reference instanceof Branch) {
      namedReference = BranchName.of(reference.getName());
      Hash hash = createReference(namedReference, reference.getHash());
      return Branch.of(reference.getName(), hash.asString());
    } else if (reference instanceof Tag) {
      namedReference = TagName.of(reference.getName());
      Hash hash = createReference(namedReference, reference.getHash());
      return Tag.of(reference.getName(), hash.asString());
    } else {
      throw new IllegalArgumentException("Only tag and branch references can be created.");
    }
  }

  private Hash createReference(NamedRef reference, String hash)
      throws NessieNotFoundException, NessieReferenceAlreadyExistsException {
    if (reference instanceof TagName && hash == null) {
      throw new IllegalArgumentException(
          "Tag-creation requires a target named-reference and hash.");
    }

    try {
      return getStore().create(reference, toHash(hash, false));
    } catch (ReferenceNotFoundException e) {
      throw new NessieReferenceNotFoundException(e.getMessage(), e);
    } catch (ReferenceAlreadyExistsException e) {
      throw new NessieReferenceAlreadyExistsException(e.getMessage(), e);
    }
  }

  @Override
  public Branch getDefaultBranch() throws NessieNotFoundException {
    Reference r =
        getReferenceByName(
            GetReferenceParams.builder().refName(getConfig().getDefaultBranch()).build());
    if (!(r instanceof Branch)) {
      throw new IllegalStateException("Default branch isn't a branch");
    }
    return (Branch) r;
  }

  @Override
  public void assignTag(String tagName, String expectedHash, Reference assignTo)
      throws NessieNotFoundException, NessieConflictException {
    assignReference(TagName.of(tagName), expectedHash, assignTo);
  }

  @Override
  public void deleteTag(String tagName, String hash)
      throws NessieConflictException, NessieNotFoundException {
    deleteReference(TagName.of(tagName), hash);
  }

  @Override
  public void assignBranch(String branchName, String expectedHash, Reference assignTo)
      throws NessieNotFoundException, NessieConflictException {
    assignReference(BranchName.of(branchName), expectedHash, assignTo);
  }

  @Override
  public void deleteBranch(String branchName, String hash)
      throws NessieConflictException, NessieNotFoundException {
    deleteReference(BranchName.of(branchName), hash);
  }

  @Override
  public LogResponse getCommitLog(String namedRef, CommitLogParams params)
      throws NessieNotFoundException {
    int max =
        Math.min(
            params.maxRecords() != null ? params.maxRecords() : MAX_COMMIT_LOG_ENTRIES,
            MAX_COMMIT_LOG_ENTRIES);

    // we should only allow named references when no paging is defined
    Ref endRef =
        namedRefWithHashOrThrow(
                namedRef, null == params.pageToken() ? params.endHash() : params.pageToken())
            .getHash();

    boolean fetchAll = FetchOption.isFetchAll(params.fetchOption());
    try (Stream<Commit<CommitMeta, Content>> commits = getStore().getCommits(endRef, fetchAll)) {
      Stream<LogEntry> logEntries =
          commits.map(
              commit -> {
                CommitMeta commitMetaWithHash =
                    addHashToCommitMeta(commit.getHash(), commit.getCommitMeta());
                ImmutableLogEntry.Builder logEntry = LogEntry.builder();
                logEntry.commitMeta(commitMetaWithHash);
                if (fetchAll) {
                  if (commit.getParentHash() != null) {
                    logEntry.parentCommitHash(commit.getParentHash().asString());
                  }
                  if (commit.getOperations() != null) {
                    commit
                        .getOperations()
                        .forEach(
                            op -> {
                              ContentKey key = ContentKey.of(op.getKey().getElements());
                              if (op instanceof Put) {
                                Content content = ((Put<Content>) op).getValue();
                                logEntry.addOperations(Operation.Put.of(key, content));
                              }
                              if (op instanceof Delete) {
                                logEntry.addOperations(Operation.Delete.of(key));
                              }
                            });
                  }
                }
                return logEntry.build();
              });

      logEntries =
          StreamSupport.stream(
              StreamUtil.takeUntilIncl(
                  logEntries.spliterator(),
                  x -> Objects.equals(x.getCommitMeta().getHash(), params.startHash())),
              false);

      List<LogEntry> items =
          filterCommitLog(logEntries, params.filter()).limit(max + 1).collect(Collectors.toList());

      if (items.size() == max + 1) {
        return ImmutableLogResponse.builder()
            .addAllLogEntries(items.subList(0, max))
            .isHasMore(true)
            .token(items.get(max).getCommitMeta().getHash())
            .build();
      }
      return ImmutableLogResponse.builder().addAllLogEntries(items).build();
    } catch (ReferenceNotFoundException e) {
      throw new NessieReferenceNotFoundException(e.getMessage(), e);
    }
  }

  private static CommitMeta addHashToCommitMeta(Hash hash, CommitMeta commitMeta) {
    return commitMeta.toBuilder().hash(hash.asString()).build();
  }

  /**
   * Applies different filters to the {@link Stream} of commits based on the filter.
   *
   * @param logEntries The commit log that different filters will be applied to
   * @param filter The filter to filter by
   * @return A potentially filtered {@link Stream} of commits based on the filter
   */
  private Stream<LogEntry> filterCommitLog(Stream<LogEntry> logEntries, String filter) {
    if (Strings.isNullOrEmpty(filter)) {
      return logEntries;
    }

    final Script script;
    try {
      script =
          SCRIPT_HOST
              .buildScript(filter)
              .withContainer(CONTAINER)
              .withDeclarations(COMMIT_LOG_DECLARATIONS)
              .withTypes(COMMIT_LOG_TYPES)
              .build();
    } catch (ScriptException e) {
      throw new IllegalArgumentException(e);
    }
    return logEntries.filter(
        logEntry -> {
          try {
            List<Operation> operations = logEntry.getOperations();
            if (operations == null) {
              operations = Collections.emptyList();
            }
            // ContentKey has some @JsonIgnore attributes, which would otherwise not be accessible.
            List<Object> operationsForCel =
                operations.stream().map(CELUtil::forCel).collect(Collectors.toList());
            return script.execute(
                Boolean.class,
                ImmutableMap.of(
                    VAR_COMMIT, logEntry.getCommitMeta(), VAR_OPERATIONS, operationsForCel));
          } catch (ScriptException e) {
            throw new RuntimeException(e);
          }
        });
  }

  @Override
  public void transplantCommitsIntoBranch(
      String branchName, String hash, String message, Transplant transplant)
      throws NessieNotFoundException, NessieConflictException {
    try {
      List<Hash> transplants;
      try (Stream<Hash> s = transplant.getHashesToTransplant().stream().map(Hash::of)) {
        transplants = s.collect(Collectors.toList());
      }
      getStore()
          .transplant(
              BranchName.of(branchName), toHash(hash, true), transplants, commitMetaUpdate());
    } catch (ReferenceNotFoundException e) {
      throw new NessieReferenceNotFoundException(e.getMessage(), e);
    } catch (ReferenceConflictException e) {
      throw new NessieReferenceConflictException(e.getMessage(), e);
    }
  }

  @Override
  public void mergeRefIntoBranch(String branchName, String hash, Merge merge)
      throws NessieNotFoundException, NessieConflictException {
    try {
      getStore()
          .merge(
              toHash(merge.getFromRefName(), merge.getFromHash()),
              BranchName.of(branchName),
              toHash(hash, true),
              commitMetaUpdate());
    } catch (ReferenceNotFoundException e) {
      throw new NessieReferenceNotFoundException(e.getMessage(), e);
    } catch (ReferenceConflictException e) {
      throw new NessieReferenceConflictException(e.getMessage(), e);
    }
  }

  @Override
  public EntriesResponse getEntries(String namedRef, EntriesParams params)
      throws NessieNotFoundException {
    Preconditions.checkArgument(params.pageToken() == null, "Paging not supported");
    WithHash<NamedRef> refWithHash = namedRefWithHashOrThrow(namedRef, params.hashOnRef());
    // TODO Implement paging. At the moment, we do not expect that many keys/entries to be returned.
    //  So the size of the whole result is probably reasonable and unlikely to "kill" either the
    //  server or client. We have to figure out _how_ to implement paging for keys/entries, i.e.
    //  whether we shall just do the whole computation for a specific hash for every page or have
    //  a more sophisticated approach, potentially with support from the (tiered-)version-store.
    //  note currently we are filtering types at the REST level. This could in theory be pushed down
    // to the store though
    //  all existing VersionStore implementations have to read all keys anyways so we don't get much
    try {
      List<EntriesResponse.Entry> entries;
      try (Stream<EntriesResponse.Entry> entryStream =
          getStore()
              .getKeys(refWithHash.getHash())
              .map(
                  key ->
                      EntriesResponse.Entry.builder()
                          .name(fromKey(key.getValue()))
                          .type((Type) key.getType())
                          .build())) {
        Stream<EntriesResponse.Entry> entriesStream = filterEntries(entryStream, params.filter());
        if (params.namespaceDepth() != null && params.namespaceDepth() > 0) {
          entriesStream =
              entriesStream
                  .filter(e -> e.getName().getElements().size() >= params.namespaceDepth())
                  .map(e -> truncate(e, params.namespaceDepth()))
                  .distinct();
        }
        entries = entriesStream.collect(ImmutableList.toImmutableList());
      }
      return EntriesResponse.builder().addAllEntries(entries).build();
    } catch (ReferenceNotFoundException e) {
      throw new NessieReferenceNotFoundException(e.getMessage(), e);
    }
  }

  private EntriesResponse.Entry truncate(EntriesResponse.Entry entry, Integer depth) {
    if (depth == null || depth < 1) {
      return entry;
    }
    Type type = entry.getName().getElements().size() > depth ? Type.UNKNOWN : entry.getType();
    ContentKey key = ContentKey.of(entry.getName().getElements().subList(0, depth));
    return EntriesResponse.Entry.builder().type(type).name(key).build();
  }

  /**
   * Applies different filters to the {@link Stream} of entries based on the filter.
   *
   * @param entries The entries that different filters will be applied to
   * @param filter The filter to filter by
   * @return A potentially filtered {@link Stream} of entries based on the filter
   */
  private Stream<EntriesResponse.Entry> filterEntries(
      Stream<EntriesResponse.Entry> entries, String filter) {
    if (Strings.isNullOrEmpty(filter)) {
      return entries;
    }

    final Script script;
    try {
      script =
          SCRIPT_HOST
              .buildScript(filter)
              .withContainer(CONTAINER)
              .withDeclarations(ENTRIES_DECLARATIONS)
              .build();
    } catch (ScriptException e) {
      throw new IllegalArgumentException(e);
    }
    return entries.filter(
        entry -> {
          // currently this is just a workaround where we put EntriesResponse.Entry into a hash
          // structure.
          // Eventually we should just be able to do "script.execute(Boolean.class, entry)"
          Map<String, Object> arguments =
              ImmutableMap.of(
                  VAR_ENTRY,
                  ImmutableMap.of(
                      VAR_NAMESPACE,
                      entry.getName().getNamespace().name(),
                      VAR_CONTENT_TYPE,
                      entry.getType().name()));

          try {
            return script.execute(Boolean.class, arguments);
          } catch (ScriptException e) {
            throw new RuntimeException(e);
          }
        });
  }

  @Override
  public Branch commitMultipleOperations(String branch, String hash, Operations operations)
      throws NessieNotFoundException, NessieConflictException {
    List<org.projectnessie.versioned.Operation<Content>> ops =
        operations.getOperations().stream()
            .map(TreeApiImpl::toOp)
            .collect(ImmutableList.toImmutableList());

    CommitMeta commitMeta = operations.getCommitMeta();
    if (commitMeta.getCommitter() != null) {
      throw new IllegalArgumentException(
          "Cannot set the committer on the client side. It is set by the server.");
    }

    try {
      Hash newHash =
          getStore()
              .commit(
                  BranchName.of(Optional.ofNullable(branch).orElse(getConfig().getDefaultBranch())),
                  Optional.ofNullable(hash).map(Hash::of),
                  commitMetaUpdate().apply(commitMeta),
                  ops);

      return Branch.of(branch, newHash.asString());
    } catch (ReferenceNotFoundException e) {
      throw new NessieReferenceNotFoundException(e.getMessage(), e);
    } catch (ReferenceConflictException e) {
      throw new NessieReferenceConflictException(e.getMessage(), e);
    }
  }

  private Function<CommitMeta, CommitMeta> commitMetaUpdate() {
    // Used for setting contextual commit properties during new and merge/transplant commits.
    // WARNING: ONLY SET PROPERTIES, WHICH APPLY COMMONLY TO ALL COMMIT TYPES.
    Principal principal = getPrincipal();
    String committer = principal == null ? "" : principal.getName();
    Instant now = Instant.now();
    return commitMeta ->
        commitMeta.toBuilder()
            .committer(committer)
            .commitTime(now)
            .author(commitMeta.getAuthor() == null ? committer : commitMeta.getAuthor())
            .authorTime(commitMeta.getAuthorTime() == null ? now : commitMeta.getAuthorTime())
            .build();
  }

  private Hash toHash(String referenceName, String hashOnReference)
      throws ReferenceNotFoundException {
    if (hashOnReference == null) {
      WithHash<Ref> hash = getStore().toRef(referenceName);
      return hash.getHash();
    }
    return toHash(hashOnReference, true)
        .orElseThrow(() -> new IllegalStateException("Required hash is missing"));
  }

  private static Optional<Hash> toHash(String hash, boolean required) {
    if (hash == null || hash.isEmpty()) {
      if (required) {
        throw new IllegalArgumentException("Must provide expected hash value for operation.");
      }
      return Optional.empty();
    }
    return Optional.of(Hash.of(hash));
  }

  protected void deleteReference(NamedRef ref, String hash)
      throws NessieConflictException, NessieNotFoundException {
    try {
      getStore().delete(ref, toHash(hash, true));
    } catch (ReferenceNotFoundException e) {
      throw new NessieReferenceNotFoundException(e.getMessage(), e);
    } catch (ReferenceConflictException e) {
      throw new NessieReferenceConflictException(e.getMessage(), e);
    }
  }

  protected void assignReference(NamedRef ref, String oldHash, Reference assignTo)
      throws NessieNotFoundException, NessieConflictException {
    try {
      WithHash<Ref> resolved = getStore().toRef(ref.getName());
      Ref resolvedRef = resolved.getValue();
      if (resolvedRef instanceof NamedRef) {
        getStore()
            .assign(
                (NamedRef) resolvedRef,
                toHash(oldHash, true),
                toHash(assignTo.getName(), assignTo.getHash()));
      } else {
        throw new IllegalArgumentException("Can only assign branch and tag types.");
      }
    } catch (ReferenceNotFoundException e) {
      throw new NessieReferenceNotFoundException(e.getMessage(), e);
    } catch (ReferenceConflictException e) {
      throw new NessieReferenceConflictException(e.getMessage(), e);
    }
  }

  private static ContentKey fromKey(Key key) {
    return ContentKey.of(key.getElements());
  }

  private static Reference makeReference(
      ReferenceInfo<CommitMeta> refWithHash, boolean fetchMetadata) {
    NamedRef ref = refWithHash.getNamedRef();
    if (ref instanceof TagName) {
      ImmutableTag.Builder builder =
          ImmutableTag.builder().name(ref.getName()).hash(refWithHash.getHash().asString());
      if (fetchMetadata) {
        builder.metadata(extractReferenceMetadata(refWithHash));
      }
      return builder.build();
    } else if (ref instanceof BranchName) {
      ImmutableBranch.Builder builder =
          ImmutableBranch.builder().name(ref.getName()).hash(refWithHash.getHash().asString());
      if (fetchMetadata) {
        builder.metadata(extractReferenceMetadata(refWithHash));
      }
      return builder.build();
    } else {
      throw new UnsupportedOperationException("only converting tags or branches"); // todo
    }
  }

  @Nullable
  private static ReferenceMetadata extractReferenceMetadata(ReferenceInfo<CommitMeta> refWithHash) {
    ImmutableReferenceMetadata.Builder builder = ImmutableReferenceMetadata.builder();
    boolean found = false;
    if (null != refWithHash.getAheadBehind()) {
      found = true;
      builder.numCommitsAhead(refWithHash.getAheadBehind().getAhead());
      builder.numCommitsBehind(refWithHash.getAheadBehind().getBehind());
    }
    if (null != refWithHash.getHeadCommitMeta()) {
      found = true;
      builder.commitMetaOfHEAD(
          addHashToCommitMeta(refWithHash.getHash(), refWithHash.getHeadCommitMeta()));
    }
    if (0L != refWithHash.getCommitSeq()) {
      found = true;
      builder.numTotalCommits(refWithHash.getCommitSeq());
    }
    if (null != refWithHash.getCommonAncestor()) {
      found = true;
      builder.commonAncestorHash(refWithHash.getCommonAncestor().asString());
    }
    if (!found) {
      return null;
    }
    return builder.build();
  }

  protected static org.projectnessie.versioned.Operation<Content> toOp(Operation o) {
    Key key = Key.of(o.getKey().getElements().toArray(new String[0]));
    if (o instanceof Operation.Delete) {
      return Delete.of(key);
    } else if (o instanceof Operation.Put) {
      Operation.Put put = (Operation.Put) o;
      return put.getExpectedContent() != null
          ? Put.of(key, put.getContent(), put.getExpectedContent())
          : Put.of(key, put.getContent());
    } else if (o instanceof Operation.Unchanged) {
      return Unchanged.of(key);
    } else {
      throw new IllegalStateException("Unknown operation " + o);
    }
  }
}

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
import static org.projectnessie.services.cel.CELUtil.ENTRIES_TYPES;
import static org.projectnessie.services.cel.CELUtil.REFERENCES_DECLARATIONS;
import static org.projectnessie.services.cel.CELUtil.REFERENCES_TYPES;
import static org.projectnessie.services.cel.CELUtil.SCRIPT_HOST;
import static org.projectnessie.services.cel.CELUtil.VAR_COMMIT;
import static org.projectnessie.services.cel.CELUtil.VAR_ENTRY;
import static org.projectnessie.services.cel.CELUtil.VAR_OPERATIONS;
import static org.projectnessie.services.cel.CELUtil.VAR_REF;
import static org.projectnessie.services.cel.CELUtil.VAR_REF_META;
import static org.projectnessie.services.cel.CELUtil.VAR_REF_TYPE;
import static org.projectnessie.services.impl.RefUtil.toNamedRef;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.security.Principal;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import javax.annotation.Nullable;
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
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.Detached;
import org.projectnessie.model.EntriesResponse;
import org.projectnessie.model.FetchOption;
import org.projectnessie.model.ImmutableBranch;
import org.projectnessie.model.ImmutableCommitMeta;
import org.projectnessie.model.ImmutableContentKeyDetails;
import org.projectnessie.model.ImmutableEntriesResponse;
import org.projectnessie.model.ImmutableLogEntry;
import org.projectnessie.model.ImmutableMergeResponse;
import org.projectnessie.model.ImmutableReferenceMetadata;
import org.projectnessie.model.ImmutableReferencesResponse;
import org.projectnessie.model.ImmutableTag;
import org.projectnessie.model.LogResponse;
import org.projectnessie.model.LogResponse.LogEntry;
import org.projectnessie.model.MergeBehavior;
import org.projectnessie.model.MergeKeyBehavior;
import org.projectnessie.model.MergeResponse;
import org.projectnessie.model.MergeResponse.ContentKeyConflict;
import org.projectnessie.model.Operation;
import org.projectnessie.model.Operations;
import org.projectnessie.model.Reference;
import org.projectnessie.model.ReferenceMetadata;
import org.projectnessie.model.ReferencesResponse;
import org.projectnessie.model.Validation;
import org.projectnessie.services.authz.Authorizer;
import org.projectnessie.services.cel.CELUtil;
import org.projectnessie.services.config.ServerConfig;
import org.projectnessie.services.spi.TreeService;
import org.projectnessie.versioned.BranchName;
import org.projectnessie.versioned.Commit;
import org.projectnessie.versioned.Delete;
import org.projectnessie.versioned.GetNamedRefsParams;
import org.projectnessie.versioned.GetNamedRefsParams.RetrieveOptions;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.Key;
import org.projectnessie.versioned.KeyEntry;
import org.projectnessie.versioned.MergeConflictException;
import org.projectnessie.versioned.MergeResult;
import org.projectnessie.versioned.MergeType;
import org.projectnessie.versioned.NamedRef;
import org.projectnessie.versioned.Put;
import org.projectnessie.versioned.ReferenceAlreadyExistsException;
import org.projectnessie.versioned.ReferenceConflictException;
import org.projectnessie.versioned.ReferenceInfo;
import org.projectnessie.versioned.ReferenceNotFoundException;
import org.projectnessie.versioned.TagName;
import org.projectnessie.versioned.Unchanged;
import org.projectnessie.versioned.VersionStore;
import org.projectnessie.versioned.WithHash;

public class TreeApiImpl extends BaseApiImpl implements TreeService {

  private static final int MAX_COMMIT_LOG_ENTRIES = 250;

  public TreeApiImpl(
      ServerConfig config,
      VersionStore store,
      Authorizer authorizer,
      Supplier<Principal> principal) {
    super(config, store, authorizer, principal);
  }

  @Override
  public ReferencesResponse getAllReferences(FetchOption fetchOption, String filter) {
    ImmutableReferencesResponse.Builder resp = ReferencesResponse.builder();
    boolean fetchAll = FetchOption.isFetchAll(fetchOption);
    try (Stream<ReferenceInfo<CommitMeta>> str =
        getStore().getNamedRefs(getGetNamedRefsParams(fetchAll))) {
      Stream<Reference> unfiltered =
          str.map(refInfo -> TreeApiImpl.makeReference(refInfo, fetchAll));
      Stream<Reference> filtered = filterReferences(unfiltered, filter);
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
  private static Stream<Reference> filterReferences(Stream<Reference> references, String filter) {
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
            return script.execute(
                Boolean.class,
                ImmutableMap.of(
                    VAR_REF,
                    reference,
                    VAR_REF_TYPE,
                    reference.getType().name(),
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
  public Reference getReferenceByName(String refName, FetchOption fetchOption)
      throws NessieNotFoundException {
    try {
      boolean fetchAll = FetchOption.isFetchAll(fetchOption);
      return makeReference(
          getStore().getNamedRef(refName, getGetNamedRefsParams(fetchAll)), fetchAll);
    } catch (ReferenceNotFoundException e) {
      throw new NessieReferenceNotFoundException(e.getMessage(), e);
    }
  }

  @Override
  public Reference createReference(
      String refName, Reference.ReferenceType type, String targetHash, String sourceRefName)
      throws NessieNotFoundException, NessieConflictException {
    Validation.validateForbiddenReferenceName(refName);
    NamedRef namedReference = toNamedRef(type, refName);
    if (type == Reference.ReferenceType.TAG && targetHash == null) {
      throw new IllegalArgumentException(
          "Tag-creation requires a target named-reference and hash.");
    }

    try {
      Hash hash = getStore().create(namedReference, toHash(targetHash, false));
      return RefUtil.toReference(namedReference, hash);
    } catch (ReferenceNotFoundException e) {
      throw new NessieReferenceNotFoundException(e.getMessage(), e);
    } catch (ReferenceAlreadyExistsException e) {
      throw new NessieReferenceAlreadyExistsException(e.getMessage(), e);
    }
  }

  @Override
  public Branch getDefaultBranch() throws NessieNotFoundException {
    Reference r = getReferenceByName(getConfig().getDefaultBranch(), FetchOption.MINIMAL);
    if (!(r instanceof Branch)) {
      throw new IllegalStateException("Default branch isn't a branch");
    }
    return (Branch) r;
  }

  @Override
  public Reference assignReference(
      Reference.ReferenceType referenceType,
      String referenceName,
      String expectedHash,
      Reference assignTo)
      throws NessieNotFoundException, NessieConflictException {
    return assignReference(toNamedRef(referenceType, referenceName), expectedHash, assignTo);
  }

  @Override
  public void deleteReference(
      Reference.ReferenceType referenceType, String referenceName, String expectedHash)
      throws NessieConflictException, NessieNotFoundException {
    deleteReference(toNamedRef(referenceType, referenceName), expectedHash);
  }

  @Override
  public LogResponse getCommitLog(
      String namedRef,
      FetchOption fetchOption,
      String oldestHashLimit,
      String youngestHash,
      String filter,
      Integer maxRecords,
      String pageToken)
      throws NessieNotFoundException {

    // we should only allow named references when no paging is defined
    WithHash<NamedRef> endRef =
        namedRefWithHashOrThrow(namedRef, null == pageToken ? youngestHash : pageToken);

    return getCommitLog(maxRecords, fetchOption, filter, endRef, oldestHashLimit);
  }

  protected LogResponse getCommitLog(
      Integer maxRecords,
      FetchOption fetchOption,
      String filter,
      WithHash<NamedRef> endRef,
      String startHash)
      throws NessieNotFoundException {
    int max =
        Math.min(maxRecords != null ? maxRecords : MAX_COMMIT_LOG_ENTRIES, MAX_COMMIT_LOG_ENTRIES);

    boolean fetchAll = FetchOption.isFetchAll(fetchOption);
    try (Stream<Commit> commits = getStore().getCommits(endRef.getHash(), fetchAll)) {
      Stream<LogEntry> logEntries = commits.map(commit -> commitToLogEntry(fetchAll, commit));

      logEntries =
          StreamSupport.stream(
              StreamUtil.takeUntilIncl(
                  logEntries.spliterator(),
                  x -> Objects.equals(x.getCommitMeta().getHash(), startHash)),
              false);

      List<LogEntry> items =
          filterCommitLog(logEntries, filter).limit(max + 1).collect(Collectors.toList());

      if (items.size() == max + 1) {
        return LogResponse.builder()
            .addAllLogEntries(items.subList(0, max))
            .isHasMore(true)
            .token(items.get(max).getCommitMeta().getHash())
            .build();
      }
      return LogResponse.builder().addAllLogEntries(items).build();
    } catch (ReferenceNotFoundException e) {
      throw new NessieReferenceNotFoundException(e.getMessage(), e);
    }
  }

  private ImmutableLogEntry commitToLogEntry(boolean fetchAll, Commit commit) {
    CommitMeta commitMetaWithHash =
        enhanceCommitMeta(commit.getHash(), commit.getCommitMeta(), commit.getAdditionalParents());
    ImmutableLogEntry.Builder logEntry = LogEntry.builder();
    logEntry.commitMeta(commitMetaWithHash);
    if (commit.getParentHash() != null) {
      logEntry.parentCommitHash(commit.getParentHash().asString());
    }

    // When we can assume that all relevant Nessie clients are aware of the
    // org.projectnessie.model.LogResponse.LogEntry.getAdditionalParents field, the following code
    // can be uncommented.
    // if (commit.getAdditionalParents() != null) {
    //   commit.getAdditionalParents().forEach(h -> logEntry.addAdditionalParents(h.asString()));
    // }

    if (fetchAll) {
      if (commit.getOperations() != null) {
        commit
            .getOperations()
            .forEach(
                op -> {
                  ContentKey key = fromKey(op.getKey());
                  if (op instanceof Put) {
                    Content content = ((Put) op).getValue();
                    logEntry.addOperations(Operation.Put.of(key, content));
                  }
                  if (op instanceof Delete) {
                    logEntry.addOperations(Operation.Delete.of(key));
                  }
                });
      }
    }
    return logEntry.build();
  }

  private static CommitMeta enhanceCommitMeta(
      Hash hash, CommitMeta commitMeta, List<Hash> additionalParents) {
    ImmutableCommitMeta.Builder updatedCommitMeta = commitMeta.toBuilder().hash(hash.asString());
    if (additionalParents != null && additionalParents.size() == 1) {
      // Only add the 1st commit ID. The MERGE_PARENT_PROPERTY was introduced for compatibility
      // with older clients. There is currently only one use case for the property: exposing the
      // commit ID of the merged commit.
      updatedCommitMeta.putProperties(
          CommitMeta.MERGE_PARENT_PROPERTY, additionalParents.get(0).asString());
    }
    return updatedCommitMeta.build();
  }

  /**
   * Applies different filters to the {@link Stream} of commits based on the filter.
   *
   * @param logEntries The commit log that different filters will be applied to
   * @param filter The filter to filter by
   * @return A potentially filtered {@link Stream} of commits based on the filter
   */
  private static Stream<LogEntry> filterCommitLog(Stream<LogEntry> logEntries, String filter) {
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
  public MergeResponse transplantCommitsIntoBranch(
      String branchName,
      String expectedHash,
      String message,
      List<String> hashesToTransplant,
      String fromRefName,
      Boolean keepIndividualCommits,
      Collection<MergeKeyBehavior> keyMergeTypes,
      MergeBehavior defaultMergeType,
      Boolean dryRun,
      Boolean fetchAdditionalInfo,
      Boolean returnConflictAsResult)
      throws NessieNotFoundException, NessieConflictException {
    try {
      List<Hash> transplants;
      try (Stream<Hash> s = hashesToTransplant.stream().map(Hash::of)) {
        transplants = s.collect(Collectors.toList());
      }

      if (Boolean.TRUE.equals(keepIndividualCommits) && transplants.size() > 1) {
        // Message overrides are not meaningful when transplanting more than one commit.
        // This matches old behaviour where `message` was ignored in all cases.
        message = null;
      }

      MergeResult<Commit> result =
          getStore()
              .transplant(
                  BranchName.of(branchName),
                  toHash(expectedHash, true),
                  transplants,
                  commitMetaUpdate(message),
                  Boolean.TRUE.equals(keepIndividualCommits),
                  keyMergeTypes(keyMergeTypes),
                  defaultMergeType(defaultMergeType),
                  Boolean.TRUE.equals(dryRun),
                  Boolean.TRUE.equals(fetchAdditionalInfo));
      return createResponse(fetchAdditionalInfo, result);
    } catch (ReferenceNotFoundException e) {
      throw new NessieReferenceNotFoundException(e.getMessage(), e);
    } catch (MergeConflictException e) {
      if (Boolean.TRUE.equals(returnConflictAsResult)) {
        @SuppressWarnings("unchecked")
        MergeResult<Commit> mr = (MergeResult<Commit>) e.getMergeResult();
        return createResponse(fetchAdditionalInfo, mr);
      }
      throw new NessieReferenceConflictException(e.getMessage(), e);
    } catch (ReferenceConflictException e) {
      throw new NessieReferenceConflictException(e.getMessage(), e);
    }
  }

  @Override
  public MergeResponse mergeRefIntoBranch(
      String branchName,
      String expectedHash,
      String fromRefName,
      String fromHash,
      Boolean keepIndividualCommits,
      @Nullable String message,
      Collection<MergeKeyBehavior> keyMergeTypes,
      MergeBehavior defaultMergeType,
      Boolean dryRun,
      Boolean fetchAdditionalInfo,
      Boolean returnConflictAsResult)
      throws NessieNotFoundException, NessieConflictException {
    try {
      MergeResult<Commit> result =
          getStore()
              .merge(
                  toHash(fromRefName, fromHash),
                  BranchName.of(branchName),
                  toHash(expectedHash, true),
                  commitMetaUpdate(message),
                  Boolean.TRUE.equals(keepIndividualCommits),
                  keyMergeTypes(keyMergeTypes),
                  defaultMergeType(defaultMergeType),
                  Boolean.TRUE.equals(dryRun),
                  Boolean.TRUE.equals(fetchAdditionalInfo));
      return createResponse(fetchAdditionalInfo, result);
    } catch (ReferenceNotFoundException e) {
      throw new NessieReferenceNotFoundException(e.getMessage(), e);
    } catch (MergeConflictException e) {
      if (Boolean.TRUE.equals(returnConflictAsResult)) {
        @SuppressWarnings("unchecked")
        MergeResult<Commit> mr = (MergeResult<Commit>) e.getMergeResult();
        return createResponse(fetchAdditionalInfo, mr);
      }
      throw new NessieReferenceConflictException(e.getMessage(), e);
    } catch (ReferenceConflictException e) {
      throw new NessieReferenceConflictException(e.getMessage(), e);
    }
  }

  private MergeResponse createResponse(Boolean fetchAdditionalInfo, MergeResult<Commit> result) {
    Function<Hash, String> hashToString = h -> h != null ? h.asString() : null;
    ImmutableMergeResponse.Builder response =
        ImmutableMergeResponse.builder()
            .targetBranch(result.getTargetBranch().getName())
            .resultantTargetHash(hashToString.apply(result.getResultantTargetHash()))
            .effectiveTargetHash(hashToString.apply(result.getEffectiveTargetHash()))
            .expectedHash(hashToString.apply(result.getExpectedHash()))
            .commonAncestor(hashToString.apply(result.getCommonAncestor()))
            .wasApplied(result.wasApplied())
            .wasSuccessful(result.wasSuccessful());

    BiConsumer<List<Commit>, Consumer<LogEntry>> convertCommits =
        (src, dest) -> {
          if (src == null) {
            return;
          }
          src.stream()
              .map(c -> commitToLogEntry(Boolean.TRUE.equals(fetchAdditionalInfo), c))
              .forEach(dest);
        };

    convertCommits.accept(result.getSourceCommits(), response::addSourceCommits);
    convertCommits.accept(result.getTargetCommits(), response::addTargetCommits);

    BiConsumer<List<Hash>, Consumer<String>> convertCommitIds =
        (src, dest) -> {
          if (src == null) {
            return;
          }
          src.stream().map(hashToString).forEach(dest);
        };

    result
        .getDetails()
        .forEach(
            (key, details) -> {
              ImmutableContentKeyDetails.Builder keyDetails =
                  ImmutableContentKeyDetails.builder()
                      .key(ContentKey.of(key.getElements()))
                      .conflictType(ContentKeyConflict.valueOf(details.getConflictType().name()))
                      .mergeBehavior(MergeBehavior.valueOf(details.getMergeType().name()));

              convertCommitIds.accept(details.getSourceCommits(), keyDetails::addSourceCommits);
              convertCommitIds.accept(details.getTargetCommits(), keyDetails::addTargetCommits);

              response.addDetails(keyDetails.build());
            });

    return response.build();
  }

  private static Map<Key, MergeType> keyMergeTypes(Collection<MergeKeyBehavior> behaviors) {
    return behaviors != null
        ? behaviors.stream()
            .collect(
                Collectors.toMap(
                    e -> toKey(e.getKey()), e -> MergeType.valueOf(e.getMergeBehavior().name())))
        : Collections.emptyMap();
  }

  private static MergeType defaultMergeType(MergeBehavior mergeBehavior) {
    return mergeBehavior != null ? MergeType.valueOf(mergeBehavior.name()) : MergeType.NORMAL;
  }

  @Override
  public EntriesResponse getEntries(
      String namedRef, String hashOnRef, Integer namespaceDepth, String filter)
      throws NessieNotFoundException {
    WithHash<NamedRef> refWithHash = namedRefWithHashOrThrow(namedRef, hashOnRef);
    // TODO Implement paging. At the moment, we do not expect that many keys/entries to be returned.
    //  So the size of the whole result is probably reasonable and unlikely to "kill" either the
    //  server or client. We have to figure out _how_ to implement paging for keys/entries, i.e.
    //  whether we shall just do the whole computation for a specific hash for every page or have
    //  a more sophisticated approach, potentially with support from the (tiered-)version-store.
    //  note currently we are filtering types at the REST level. This could in theory be pushed down
    // to the store though
    //  all existing VersionStore implementations have to read all keys anyways so we don't get much
    try {
      ImmutableEntriesResponse.Builder response = EntriesResponse.builder();
      try (Stream<KeyEntry> entryStream = getStore().getKeys(refWithHash.getHash())) {
        Stream<EntriesResponse.Entry> entriesStream =
            filterEntries(refWithHash, entryStream, filter)
                .map(key -> EntriesResponse.Entry.entry(fromKey(key.getKey()), key.getType()));
        if (namespaceDepth != null && namespaceDepth > 0) {
          entriesStream =
              entriesStream
                  .filter(e -> e.getName().getElements().size() >= namespaceDepth)
                  .map(e -> truncate(e, namespaceDepth))
                  .distinct();
        }
        entriesStream.forEach(response::addEntries);
      }
      return response.build();
    } catch (ReferenceNotFoundException e) {
      throw new NessieReferenceNotFoundException(e.getMessage(), e);
    }
  }

  private static EntriesResponse.Entry truncate(EntriesResponse.Entry entry, Integer depth) {
    if (depth == null || depth < 1) {
      return entry;
    }
    Content.Type type =
        entry.getName().getElements().size() > depth ? Content.Type.NAMESPACE : entry.getType();
    ContentKey key = ContentKey.of(entry.getName().getElements().subList(0, depth));
    return EntriesResponse.Entry.entry(key, type);
  }

  /**
   * Applies different filters to the {@link Stream} of entries based on the filter.
   *
   * @param entries The entries that different filters will be applied to
   * @param filter The filter to filter by
   * @return A potentially filtered {@link Stream} of entries based on the filter
   */
  protected Stream<KeyEntry> filterEntries(
      WithHash<NamedRef> refWithHash, Stream<KeyEntry> entries, String filter) {
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
              .withTypes(ENTRIES_TYPES)
              .build();
    } catch (ScriptException e) {
      throw new IllegalArgumentException(e);
    }
    return entries.filter(
        entry -> {
          Map<String, Object> arguments = ImmutableMap.of(VAR_ENTRY, CELUtil.forCel(entry));

          try {
            return script.execute(Boolean.class, arguments);
          } catch (ScriptException e) {
            throw new RuntimeException(e);
          }
        });
  }

  @Override
  public Branch commitMultipleOperations(String branch, String expectedHash, Operations operations)
      throws NessieNotFoundException, NessieConflictException {
    List<org.projectnessie.versioned.Operation> ops =
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
                  Optional.ofNullable(expectedHash).map(Hash::of),
                  commitMetaUpdate(null).rewriteSingle(commitMeta),
                  ops);

      return Branch.of(branch, newHash.asString());
    } catch (ReferenceNotFoundException e) {
      throw new NessieReferenceNotFoundException(e.getMessage(), e);
    } catch (ReferenceConflictException e) {
      throw new NessieReferenceConflictException(e.getMessage(), e);
    }
  }

  private Hash toHash(String referenceName, String hashOnReference)
      throws ReferenceNotFoundException {
    if (Detached.REF_NAME.equals(referenceName)) {
      return Hash.of(hashOnReference);
    }
    if (hashOnReference == null) {
      return getStore().getNamedRef(referenceName, GetNamedRefsParams.DEFAULT).getHash();
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

  protected void deleteReference(NamedRef ref, String expectedHash)
      throws NessieConflictException, NessieNotFoundException {
    try {
      getStore().delete(ref, toHash(expectedHash, true));
    } catch (ReferenceNotFoundException e) {
      throw new NessieReferenceNotFoundException(e.getMessage(), e);
    } catch (ReferenceConflictException e) {
      throw new NessieReferenceConflictException(e.getMessage(), e);
    }
  }

  protected Reference assignReference(NamedRef ref, String expectedHash, Reference assignTo)
      throws NessieNotFoundException, NessieConflictException {
    try {
      ReferenceInfo<CommitMeta> resolved =
          getStore().getNamedRef(ref.getName(), GetNamedRefsParams.DEFAULT);

      Hash targetHash = toHash(assignTo.getName(), assignTo.getHash());
      getStore().assign(resolved.getNamedRef(), toHash(expectedHash, true), targetHash);
      return RefUtil.toReference(ref, targetHash);
    } catch (ReferenceNotFoundException e) {
      throw new NessieReferenceNotFoundException(e.getMessage(), e);
    } catch (ReferenceConflictException e) {
      throw new NessieReferenceConflictException(e.getMessage(), e);
    }
  }

  protected static ContentKey fromKey(Key key) {
    return ContentKey.of(key.getElements());
  }

  public static Key toKey(ContentKey key) {
    return Key.of(key.getElements());
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
          enhanceCommitMeta(refWithHash.getHash(), refWithHash.getHeadCommitMeta(), null));
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

  protected static org.projectnessie.versioned.Operation toOp(Operation o) {
    Key key = toKey(o.getKey());
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

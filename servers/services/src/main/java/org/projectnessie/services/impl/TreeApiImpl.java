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

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Sets.newHashSetWithExpectedSize;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singleton;
import static org.projectnessie.model.CommitResponse.AddedContent.addedContent;
import static org.projectnessie.services.authz.Check.canReadContentKey;
import static org.projectnessie.services.authz.Check.canReadEntries;
import static org.projectnessie.services.authz.Check.canViewReference;
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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
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
import org.projectnessie.model.CommitResponse;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.Detached;
import org.projectnessie.model.EntriesResponse;
import org.projectnessie.model.FetchOption;
import org.projectnessie.model.ImmutableBranch;
import org.projectnessie.model.ImmutableCommitMeta;
import org.projectnessie.model.ImmutableCommitResponse;
import org.projectnessie.model.ImmutableContentKeyDetails;
import org.projectnessie.model.ImmutableLogEntry;
import org.projectnessie.model.ImmutableMergeResponse;
import org.projectnessie.model.ImmutableReferenceMetadata;
import org.projectnessie.model.ImmutableTag;
import org.projectnessie.model.LogResponse.LogEntry;
import org.projectnessie.model.MergeBehavior;
import org.projectnessie.model.MergeKeyBehavior;
import org.projectnessie.model.MergeResponse;
import org.projectnessie.model.MergeResponse.ContentKeyConflict;
import org.projectnessie.model.Operation;
import org.projectnessie.model.Operations;
import org.projectnessie.model.Reference;
import org.projectnessie.model.Reference.ReferenceType;
import org.projectnessie.model.ReferenceMetadata;
import org.projectnessie.model.Validation;
import org.projectnessie.services.authz.Authorizer;
import org.projectnessie.services.authz.AuthzPaginationIterator;
import org.projectnessie.services.authz.BatchAccessChecker;
import org.projectnessie.services.authz.Check;
import org.projectnessie.services.cel.CELUtil;
import org.projectnessie.services.config.ServerConfig;
import org.projectnessie.services.spi.PagedResponseHandler;
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
import org.projectnessie.versioned.paging.PaginationIterator;

public class TreeApiImpl extends BaseApiImpl implements TreeService {

  public TreeApiImpl(
      ServerConfig config,
      VersionStore store,
      Authorizer authorizer,
      Supplier<Principal> principal) {
    super(config, store, authorizer, principal);
  }

  @Override
  public <R> R getAllReferences(
      FetchOption fetchOption,
      String filter,
      String pagingToken,
      PagedResponseHandler<R, Reference> pagedResponseHandler) {
    boolean fetchAll = FetchOption.isFetchAll(fetchOption);
    try (PaginationIterator<ReferenceInfo<CommitMeta>> references =
        getStore().getNamedRefs(getGetNamedRefsParams(fetchAll), pagingToken)) {

      AuthzPaginationIterator<ReferenceInfo<CommitMeta>> authz =
          new AuthzPaginationIterator<ReferenceInfo<CommitMeta>>(
              references, super::startAccessCheck, ACCESS_CHECK_BATCH_SIZE) {
            @Override
            protected Set<Check> checksForEntry(ReferenceInfo<CommitMeta> entry) {
              return singleton(canViewReference(entry.getNamedRef()));
            }
          };

      Predicate<Reference> filterPredicate = filterReferences(filter);
      while (authz.hasNext()) {
        ReferenceInfo<CommitMeta> refInfo = authz.next();
        Reference ref = makeReference(refInfo, fetchAll);
        if (!filterPredicate.test(ref)) {
          continue;
        }
        if (!pagedResponseHandler.addEntry(ref)) {
          pagedResponseHandler.hasMore(authz.tokenForCurrent());
          break;
        }
      }
    } catch (ReferenceNotFoundException e) {
      throw new IllegalArgumentException(
          String.format(
              "Could not find default branch '%s'.", this.getConfig().getDefaultBranch()));
    }
    return pagedResponseHandler.build();
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
   * Produces the filter predicate for reference-filtering.
   *
   * @param filter The filter to filter by
   */
  private static Predicate<Reference> filterReferences(String filter) {
    if (Strings.isNullOrEmpty(filter)) {
      return r -> true;
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
    return reference -> {
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
    };
  }

  @Override
  public Reference getReferenceByName(String refName, FetchOption fetchOption)
      throws NessieNotFoundException {
    try {
      boolean fetchAll = FetchOption.isFetchAll(fetchOption);
      Reference ref =
          makeReference(getStore().getNamedRef(refName, getGetNamedRefsParams(fetchAll)), fetchAll);
      startAccessCheck().canViewReference(RefUtil.toNamedRef(ref)).checkAndThrow();
      return ref;
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

    BatchAccessChecker check =
        startAccessCheck().canCreateReference(RefUtil.toNamedRef(type, refName));
    try {
      check.canViewReference(namedRefWithHashOrThrow(sourceRefName, targetHash).getValue());
    } catch (NessieNotFoundException e) {
      // If the default-branch does not exist and hashOnRef points to the "beginning of time",
      // then do not throw a NessieNotFoundException, but re-create the default branch. In all
      // cases, re-throw the exception.
      if (!(Reference.ReferenceType.BRANCH.equals(type)
          && refName.equals(getConfig().getDefaultBranch())
          && (null == targetHash || getStore().noAncestorHash().asString().equals(targetHash)))) {
        throw e;
      }
    }
    check.checkAndThrow();

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
    checkState(r instanceof Branch, "Default branch isn't a branch");
    return (Branch) r;
  }

  @Override
  public Reference assignReference(
      Reference.ReferenceType referenceType,
      String referenceName,
      String expectedHash,
      Reference assignTo)
      throws NessieNotFoundException, NessieConflictException {
    try {
      NamedRef ref = toNamedRef(referenceType, referenceName);

      ReferenceInfo<CommitMeta> resolved =
          getStore().getNamedRef(ref.getName(), GetNamedRefsParams.DEFAULT);

      startAccessCheck()
          .canViewReference(
              namedRefWithHashOrThrow(assignTo.getName(), assignTo.getHash()).getValue())
          .canAssignRefToHash(ref)
          .checkAndThrow();

      Hash targetHash = toHash(assignTo.getName(), assignTo.getHash());
      getStore().assign(resolved.getNamedRef(), toHash(expectedHash, true), targetHash);
      return RefUtil.toReference(ref, targetHash);
    } catch (ReferenceNotFoundException e) {
      throw new NessieReferenceNotFoundException(e.getMessage(), e);
    } catch (ReferenceConflictException e) {
      throw new NessieReferenceConflictException(e.getMessage(), e);
    }
  }

  @Override
  public String deleteReference(
      ReferenceType referenceType, String referenceName, String expectedHash)
      throws NessieConflictException, NessieNotFoundException {
    try {
      NamedRef ref = toNamedRef(referenceType, referenceName);

      if (ref instanceof BranchName && getConfig().getDefaultBranch().equals(ref.getName())) {
        throw new IllegalArgumentException(
            "Default branch '" + ref.getName() + "' cannot be deleted.");
      }
      startAccessCheck().canDeleteReference(ref).checkAndThrow();

      return getStore().delete(ref, toHash(expectedHash, true)).asString();
    } catch (ReferenceNotFoundException e) {
      throw new NessieReferenceNotFoundException(e.getMessage(), e);
    } catch (ReferenceConflictException e) {
      throw new NessieReferenceConflictException(e.getMessage(), e);
    }
  }

  @Override
  public <R> R getCommitLog(
      String namedRef,
      FetchOption fetchOption,
      String oldestHashLimit,
      String youngestHash,
      String filter,
      String pageToken,
      PagedResponseHandler<R, LogEntry> pagedResponseHandler)
      throws NessieNotFoundException {
    // we should only allow named references when no paging is defined
    WithHash<NamedRef> endRef =
        namedRefWithHashOrThrow(namedRef, null == pageToken ? youngestHash : pageToken);

    startAccessCheck().canListCommitLog(endRef.getValue()).checkAndThrow();

    boolean fetchAll = FetchOption.isFetchAll(fetchOption);
    Set<Check> successfulChecks = new HashSet<>();
    Set<Check> failedChecks = new HashSet<>();
    try (PaginationIterator<Commit> commits = getStore().getCommits(endRef.getHash(), fetchAll)) {

      Predicate<LogEntry> predicate = filterCommitLog(filter);
      while (commits.hasNext()) {
        Commit commit = commits.next();

        LogEntry logEntry = commitToLogEntry(fetchAll, commit);

        String hash = logEntry.getCommitMeta().getHash();

        if (!predicate.test(logEntry)) {
          continue;
        }

        boolean stop = Objects.equals(hash, oldestHashLimit);

        logEntry = logEntryOperationsAccessCheck(successfulChecks, failedChecks, endRef, logEntry);

        if (!pagedResponseHandler.addEntry(logEntry)) {
          if (!stop) {
            pagedResponseHandler.hasMore(commits.tokenForCurrent());
          }
          break;
        }

        if (stop) {
          break;
        }
      }

      return pagedResponseHandler.build();
    } catch (ReferenceNotFoundException e) {
      throw new NessieReferenceNotFoundException(e.getMessage(), e);
    }
  }

  private LogEntry logEntryOperationsAccessCheck(
      Set<Check> successfulChecks,
      Set<Check> failedChecks,
      WithHash<NamedRef> endRef,
      LogEntry logEntry) {
    List<Operation> operations = logEntry.getOperations();
    if (operations == null || operations.isEmpty()) {
      return logEntry;
    }

    ImmutableLogEntry.Builder newLogEntry =
        ImmutableLogEntry.builder().from(logEntry).operations(emptyList());

    Set<Check> checks = newHashSetWithExpectedSize(operations.size());
    for (Operation op : operations) {
      if (op instanceof Operation.Put) {
        Operation.Put put = (Operation.Put) op;
        checks.add(canReadContentKey(endRef.getValue(), put.getKey(), put.getContent().getId()));
      } else if (op instanceof Operation.Delete) {
        Operation.Delete delete = (Operation.Delete) op;
        checks.add(canReadContentKey(endRef.getValue(), delete.getKey(), null));
      } else {
        throw new IllegalStateException("Unknown operation " + op);
      }
    }

    BatchAccessChecker accessCheck = startAccessCheck();
    boolean anyCheck = false;
    for (Check check : checks) {
      if (!successfulChecks.contains(check) && !failedChecks.contains(check)) {
        accessCheck.can(check);
        anyCheck = true;
      }
    }
    Map<Check, String> failures = anyCheck ? accessCheck.check() : emptyMap();

    for (Operation op : operations) {
      Check check;
      if (op instanceof Operation.Put) {
        Operation.Put put = (Operation.Put) op;
        check = canReadContentKey(endRef.getValue(), put.getKey(), put.getContent().getId());
      } else if (op instanceof Operation.Delete) {
        Operation.Delete delete = (Operation.Delete) op;
        check = canReadContentKey(endRef.getValue(), delete.getKey(), null);
      } else {
        throw new IllegalStateException("Unknown operation " + op);
      }
      if (failures.containsKey(check)) {
        failedChecks.add(check);
      } else if (!failedChecks.contains(check)) {
        newLogEntry.addOperations(op);
        successfulChecks.add(check);
      }
    }
    return newLogEntry.build();
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
   * Produces the filter predicate for commit-log filtering.
   *
   * @param filter The filter to filter by
   */
  private static Predicate<LogEntry> filterCommitLog(String filter) {
    if (Strings.isNullOrEmpty(filter)) {
      return x -> true;
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
    return logEntry -> {
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
    };
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
      if (hashesToTransplant.isEmpty()) {
        throw new IllegalArgumentException("No hashes given to transplant.");
      }

      BranchName targetBranch = BranchName.of(branchName);
      startAccessCheck()
          .canViewReference(
              namedRefWithHashOrThrow(
                      fromRefName, hashesToTransplant.get(hashesToTransplant.size() - 1))
                  .getValue())
          .canCommitChangeAgainstReference(targetBranch)
          .checkAndThrow();

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
                  targetBranch,
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
      BranchName targetBranch = BranchName.of(branchName);
      startAccessCheck()
          .canViewReference(namedRefWithHashOrThrow(fromRefName, fromHash).getValue())
          .canCommitChangeAgainstReference(targetBranch)
          .checkAndThrow();

      MergeResult<Commit> result =
          getStore()
              .merge(
                  toHash(fromRefName, fromHash),
                  targetBranch,
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
  public <R> R getEntries(
      String namedRef,
      String hashOnRef,
      Integer namespaceDepth,
      String filter,
      String pagingToken,
      PagedResponseHandler<R, EntriesResponse.Entry> pagedResponseHandler)
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
      Predicate<KeyEntry> filterPredicate = filterEntries(filter);

      try (PaginationIterator<KeyEntry> entries =
          getStore().getKeys(refWithHash.getHash(), pagingToken)) {

        AuthzPaginationIterator<KeyEntry> authz =
            new AuthzPaginationIterator<KeyEntry>(
                entries, super::startAccessCheck, ACCESS_CHECK_BATCH_SIZE) {
              @Override
              protected Set<Check> checksForEntry(KeyEntry entry) {
                return singleton(
                    canReadContentKey(
                        refWithHash.getValue(), fromKey(entry.getKey()), entry.getContentId()));
              }
            }.initialCheck(canReadEntries(refWithHash.getValue()));

        if (namespaceDepth != null && namespaceDepth > 0) {
          int depth = namespaceDepth;
          filterPredicate = filterPredicate.and(e -> e.getKey().getElements().size() >= depth);
          Set<ContentKey> seen = new HashSet<>();
          while (authz.hasNext()) {
            KeyEntry key = authz.next();

            if (!filterPredicate.test(key)) {
              continue;
            }

            EntriesResponse.Entry entry =
                EntriesResponse.Entry.entry(
                    fromKey(key.getKey()), key.getType(), key.getContentId());

            entry = namespaceDepthMapping(entry, depth);

            if (seen.add(entry.getName())) {
              if (!pagedResponseHandler.addEntry(entry)) {
                pagedResponseHandler.hasMore(authz.tokenForCurrent());
                break;
              }
            }
          }
        } else {
          while (authz.hasNext()) {
            KeyEntry key = authz.next();

            if (!filterPredicate.test(key)) {
              continue;
            }

            EntriesResponse.Entry entry =
                EntriesResponse.Entry.entry(
                    fromKey(key.getKey()), key.getType(), key.getContentId());

            if (!pagedResponseHandler.addEntry(entry)) {
              pagedResponseHandler.hasMore(authz.tokenForCurrent());
              break;
            }
          }
        }
      }
      return pagedResponseHandler.build();
    } catch (ReferenceNotFoundException e) {
      throw new NessieReferenceNotFoundException(e.getMessage(), e);
    }
  }

  private static EntriesResponse.Entry namespaceDepthMapping(
      EntriesResponse.Entry entry, int depth) {
    Content.Type type =
        entry.getName().getElements().size() > depth ? Content.Type.NAMESPACE : entry.getType();
    ContentKey key = ContentKey.of(entry.getName().getElements().subList(0, depth));
    return EntriesResponse.Entry.entry(key, type, null);
  }

  /**
   * Produces the predicate for key-entry filtering.
   *
   * @param filter The filter to filter by
   */
  protected Predicate<KeyEntry> filterEntries(String filter) {
    if (Strings.isNullOrEmpty(filter)) {
      return x -> true;
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
    return entry -> {
      Map<String, Object> arguments = ImmutableMap.of(VAR_ENTRY, CELUtil.forCel(entry));

      try {
        return script.execute(Boolean.class, arguments);
      } catch (ScriptException e) {
        throw new RuntimeException(e);
      }
    };
  }

  @Override
  public CommitResponse commitMultipleOperations(
      String branch, String expectedHash, Operations operations)
      throws NessieNotFoundException, NessieConflictException {
    BranchName branchName = BranchName.of(branch);
    BatchAccessChecker check = startAccessCheck().canCommitChangeAgainstReference(branchName);
    operations
        .getOperations()
        .forEach(
            op -> {
              if (op instanceof Operation.Delete) {
                check.canDeleteEntity(branchName, op.getKey(), null);
              } else if (op instanceof Operation.Put) {
                Operation.Put putOp = (Operation.Put) op;
                check.canUpdateEntity(
                    branchName,
                    op.getKey(),
                    putOp.getContent().getId(),
                    putOp.getContent().getType());
              }
            });
    check.checkAndThrow();

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
      ImmutableCommitResponse.Builder commitResponse = ImmutableCommitResponse.builder();

      Hash newHash =
          getStore()
              .commit(
                  BranchName.of(branch),
                  Optional.ofNullable(expectedHash).map(Hash::of),
                  commitMetaUpdate(null).rewriteSingle(commitMeta),
                  ops,
                  () -> null,
                  (key, cid) -> {
                    commitResponse.addAddedContents(
                        addedContent(ContentKey.of(key.getElements()), cid));
                  });

      return commitResponse.targetBranch(Branch.of(branch, newHash.asString())).build();
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

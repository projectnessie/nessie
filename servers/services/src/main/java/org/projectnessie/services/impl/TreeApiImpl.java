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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Sets.newHashSetWithExpectedSize;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singleton;
import static java.util.function.Function.identity;
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
import static org.projectnessie.versioned.RequestMeta.API_WRITE;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import jakarta.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiPredicate;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
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
import org.projectnessie.model.EntriesResponse.Entry;
import org.projectnessie.model.FetchOption;
import org.projectnessie.model.IdentifiedContentKey;
import org.projectnessie.model.ImmutableCommitResponse;
import org.projectnessie.model.ImmutableContentKeyDetails;
import org.projectnessie.model.ImmutableLogEntry;
import org.projectnessie.model.ImmutableMergeResponse;
import org.projectnessie.model.ImmutableReferenceHistoryResponse;
import org.projectnessie.model.ImmutableReferenceMetadata;
import org.projectnessie.model.LogResponse.LogEntry;
import org.projectnessie.model.MergeBehavior;
import org.projectnessie.model.MergeKeyBehavior;
import org.projectnessie.model.MergeResponse;
import org.projectnessie.model.MergeResponse.ContentKeyConflict;
import org.projectnessie.model.Operation;
import org.projectnessie.model.Operation.Delete;
import org.projectnessie.model.Operation.Put;
import org.projectnessie.model.Operations;
import org.projectnessie.model.Reference;
import org.projectnessie.model.Reference.ReferenceType;
import org.projectnessie.model.ReferenceHistoryResponse;
import org.projectnessie.model.ReferenceHistoryState;
import org.projectnessie.model.ReferenceMetadata;
import org.projectnessie.model.Tag;
import org.projectnessie.model.Validation;
import org.projectnessie.services.authz.AccessContext;
import org.projectnessie.services.authz.ApiContext;
import org.projectnessie.services.authz.Authorizer;
import org.projectnessie.services.authz.AuthzPaginationIterator;
import org.projectnessie.services.authz.BatchAccessChecker;
import org.projectnessie.services.authz.Check;
import org.projectnessie.services.authz.RetriableAccessChecker;
import org.projectnessie.services.cel.CELUtil;
import org.projectnessie.services.config.ServerConfig;
import org.projectnessie.services.hash.HashValidator;
import org.projectnessie.services.hash.ResolvedHash;
import org.projectnessie.services.spi.PagedResponseHandler;
import org.projectnessie.services.spi.TreeService;
import org.projectnessie.versioned.BranchName;
import org.projectnessie.versioned.Commit;
import org.projectnessie.versioned.GetNamedRefsParams;
import org.projectnessie.versioned.GetNamedRefsParams.RetrieveOptions;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.KeyEntry;
import org.projectnessie.versioned.MergeConflictException;
import org.projectnessie.versioned.MergeResult;
import org.projectnessie.versioned.MergeTransplantResultBase;
import org.projectnessie.versioned.NamedRef;
import org.projectnessie.versioned.ReferenceAlreadyExistsException;
import org.projectnessie.versioned.ReferenceConflictException;
import org.projectnessie.versioned.ReferenceHistory;
import org.projectnessie.versioned.ReferenceInfo;
import org.projectnessie.versioned.ReferenceNotFoundException;
import org.projectnessie.versioned.RequestMeta;
import org.projectnessie.versioned.TagName;
import org.projectnessie.versioned.TransplantResult;
import org.projectnessie.versioned.VersionStore;
import org.projectnessie.versioned.VersionStore.CommitValidator;
import org.projectnessie.versioned.VersionStore.MergeOp;
import org.projectnessie.versioned.VersionStore.TransplantOp;
import org.projectnessie.versioned.WithHash;
import org.projectnessie.versioned.paging.PaginationIterator;

public class TreeApiImpl extends BaseApiImpl implements TreeService {

  public TreeApiImpl(
      ServerConfig config,
      VersionStore store,
      Authorizer authorizer,
      AccessContext accessContext,
      ApiContext apiContext) {
    super(config, store, authorizer, accessContext, apiContext);
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
              references, super::startAccessCheck, getServerConfig().accessChecksBatchSize()) {
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
              "Could not find default branch '%s'.", this.getServerConfig().getDefaultBranch()));
    }
    return pagedResponseHandler.build();
  }

  private GetNamedRefsParams getGetNamedRefsParams(boolean fetchMetadata) {
    return fetchMetadata
        ? GetNamedRefsParams.builder()
            .baseReference(BranchName.of(this.getServerConfig().getDefaultBranch()))
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
  public ReferenceHistoryResponse getReferenceHistory(String refName, Integer headCommitsToScan)
      throws NessieNotFoundException {
    Reference ref;
    ReferenceHistory history;
    try {
      ref = makeReference(getStore().getNamedRef(refName, getGetNamedRefsParams(false)), false);

      startAccessCheck().canViewReference(RefUtil.toNamedRef(ref)).checkAndThrow();

      history = getStore().getReferenceHistory(ref.getName(), headCommitsToScan);
    } catch (ReferenceNotFoundException e) {
      throw new NessieReferenceNotFoundException(e.getMessage(), e);
    }

    ImmutableReferenceHistoryResponse.Builder response =
        ReferenceHistoryResponse.builder()
            .reference(ref)
            .commitLogConsistency(history.commitLogConsistency());
    response.current(convertStoreHistoryEntry(history.current()));
    history.previous().stream().map(this::convertStoreHistoryEntry).forEach(response::addPrevious);
    return response.build();
  }

  private ReferenceHistoryState convertStoreHistoryEntry(
      ReferenceHistory.ReferenceHistoryElement element) {
    return ReferenceHistoryState.referenceHistoryElement(
        element.pointer().asString(), element.commitConsistency(), element.meta());
  }

  @Override
  public Reference createReference(
      String refName, ReferenceType type, String targetHash, String sourceRefName)
      throws NessieNotFoundException, NessieConflictException {
    Validation.validateForbiddenReferenceName(refName);
    NamedRef namedReference = toNamedRef(type, refName);

    BatchAccessChecker check = startAccessCheck().canCreateReference(namedReference);
    Optional<Hash> targetHashObj;
    try {
      ResolvedHash targetRef =
          getHashResolver()
              .resolveHashOnRef(
                  sourceRefName,
                  targetHash,
                  new HashValidator("Target hash").hashMustBeUnambiguous());
      check.canViewReference(targetRef.getNamedRef());
      targetHashObj = Optional.of(targetRef.getHash());
    } catch (ReferenceNotFoundException e) {
      // If the default-branch does not exist and hashOnRef points to the "beginning of time",
      // then do not throw a NessieNotFoundException, but re-create the default branch. In all
      // cases, re-throw the exception.
      if (!(ReferenceType.BRANCH.equals(type)
          && refName.equals(getServerConfig().getDefaultBranch())
          && (null == targetHash || getStore().noAncestorHash().asString().equals(targetHash)))) {
        throw new NessieReferenceNotFoundException(e.getMessage(), e);
      }
      targetHashObj = Optional.empty();
    }
    check.checkAndThrow();

    try {
      Hash hash = getStore().create(namedReference, targetHashObj).getHash();
      return RefUtil.toReference(namedReference, hash);
    } catch (ReferenceNotFoundException e) {
      throw new NessieReferenceNotFoundException(e.getMessage(), e);
    } catch (ReferenceAlreadyExistsException e) {
      throw new NessieReferenceAlreadyExistsException(e.getMessage(), e);
    }
  }

  @Override
  public Branch getDefaultBranch() throws NessieNotFoundException {
    Reference r = getReferenceByName(getServerConfig().getDefaultBranch(), FetchOption.MINIMAL);
    checkState(r instanceof Branch, "Default branch isn't a branch");
    return (Branch) r;
  }

  @Override
  public Reference assignReference(
      ReferenceType referenceType, String referenceName, String expectedHash, Reference assignTo)
      throws NessieNotFoundException, NessieConflictException {
    try {

      ResolvedHash oldRef =
          getHashResolver()
              .resolveHashOnRef(
                  referenceName,
                  expectedHash,
                  new HashValidator("Assignment target", "Expected hash")
                      .refMustBeBranchOrTag()
                      .hashMustBeUnambiguous());
      ResolvedHash newRef =
          getHashResolver()
              .resolveHashOnRef(
                  assignTo.getName(),
                  assignTo.getHash(),
                  new HashValidator("Target hash").hashMustBeUnambiguous());

      checkArgument(
          referenceType == null || referenceType == RefUtil.referenceType(oldRef.getNamedRef()),
          "Expected reference type %s does not match existing reference %s",
          referenceType,
          oldRef.getNamedRef());

      startAccessCheck()
          .canViewReference(newRef.getNamedRef())
          .canAssignRefToHash(oldRef.getNamedRef())
          .checkAndThrow();

      getStore().assign(oldRef.getNamedRef(), oldRef.getHash(), newRef.getHash());
      return RefUtil.toReference(oldRef.getNamedRef(), newRef.getHash());
    } catch (ReferenceNotFoundException e) {
      throw new NessieReferenceNotFoundException(e.getMessage(), e);
    } catch (ReferenceConflictException e) {
      throw new NessieReferenceConflictException(e.getReferenceConflicts(), e.getMessage(), e);
    }
  }

  @Override
  public Reference deleteReference(
      ReferenceType referenceType, String referenceName, String expectedHash)
      throws NessieConflictException, NessieNotFoundException {
    try {
      ReferenceInfo<CommitMeta> resolved =
          getStore().getNamedRef(referenceName, GetNamedRefsParams.DEFAULT);
      NamedRef ref = resolved.getNamedRef();

      checkArgument(
          referenceType == null || referenceType == RefUtil.referenceType(ref),
          "Expected reference type %s does not match existing reference %s",
          referenceType,
          ref);
      checkArgument(
          !(ref instanceof BranchName
              && getServerConfig().getDefaultBranch().equals(ref.getName())),
          "Default branch '%s' cannot be deleted.",
          ref.getName());

      startAccessCheck().canDeleteReference(ref).checkAndThrow();

      ResolvedHash refToDelete =
          getHashResolver()
              .resolveHashOnRef(
                  resolved.getNamedRef(),
                  resolved.getHash(),
                  expectedHash,
                  new HashValidator("Expected hash").hashMustBeUnambiguous());

      Hash deletedAthash = getStore().delete(ref, refToDelete.getHash()).getHash();
      return RefUtil.toReference(ref, deletedAthash);
    } catch (ReferenceNotFoundException e) {
      throw new NessieReferenceNotFoundException(e.getMessage(), e);
    } catch (ReferenceConflictException e) {
      throw new NessieReferenceConflictException(e.getReferenceConflicts(), e.getMessage(), e);
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

    try {

      ResolvedHash endRef =
          getHashResolver()
              .resolveHashOnRef(
                  namedRef,
                  null == pageToken ? youngestHash : pageToken,
                  new HashValidator(null == pageToken ? "Youngest hash" : "Token pagination hash"));

      startAccessCheck().canListCommitLog(endRef.getNamedRef()).checkAndThrow();

      String stopHash =
          oldestHashLimit == null
              ? null
              : getHashResolver()
                  .resolveHashOnRef(endRef, oldestHashLimit, new HashValidator("Oldest hash"))
                  .getHash()
                  .asString();

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

          boolean stop = Objects.equals(hash, stopHash);

          logEntry =
              logEntryOperationsAccessCheck(successfulChecks, failedChecks, endRef, logEntry);

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

    Map<ContentKey, IdentifiedContentKey> identifiedKeys = new HashMap<>();

    try {
      getStore()
          .getIdentifiedKeys(
              endRef.getHash(),
              operations.stream().map(Operation::getKey).collect(Collectors.toList()))
          .forEach(entry -> identifiedKeys.put(entry.contentKey(), entry));
    } catch (ReferenceNotFoundException e) {
      throw new RuntimeException(e);
    }

    Set<Check> checks = newHashSetWithExpectedSize(operations.size());
    for (Operation op : operations) {
      if (!(op instanceof Operation.Put) && !(op instanceof Operation.Delete)) {
        throw new IllegalStateException("Unknown operation " + op);
      }
      IdentifiedContentKey identifiedKey = identifiedKeys.get(op.getKey());
      if (identifiedKey != null) {
        checks.add(canReadContentKey(endRef.getValue(), identifiedKey));
      } else {
        checks.add(canReadContentKey(endRef.getValue(), op.getKey()));
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
      if (!(op instanceof Operation.Put) && !(op instanceof Operation.Delete)) {
        throw new IllegalStateException("Unknown operation " + op);
      }
      IdentifiedContentKey identifiedKey = identifiedKeys.get(op.getKey());
      Check check;
      if (identifiedKey != null) {
        check = canReadContentKey(endRef.getValue(), identifiedKey);
      } else {
        check = canReadContentKey(endRef.getValue(), op.getKey());
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
    CommitMeta commitMeta = commit.getCommitMeta();
    ImmutableLogEntry.Builder logEntry = LogEntry.builder();
    logEntry.commitMeta(commitMeta);
    if (commit.getParentHash() != null) {
      logEntry.parentCommitHash(commit.getParentHash().asString());
    }

    if (fetchAll) {
      if (commit.getOperations() != null) {
        commit
            .getOperations()
            .forEach(
                op -> {
                  ContentKey key = op.getKey();
                  if (op instanceof Put) {
                    Content content = ((Put) op).getContent();
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
      @Nullable CommitMeta commitMeta,
      List<String> hashesToTransplant,
      String fromRefName,
      Collection<MergeKeyBehavior> keyMergeBehaviors,
      MergeBehavior defaultMergeBehavior,
      Boolean dryRun,
      Boolean fetchAdditionalInfo,
      Boolean returnConflictAsResult)
      throws NessieNotFoundException, NessieConflictException {
    try {
      checkArgument(!hashesToTransplant.isEmpty(), "No hashes given to transplant.");
      validateCommitMeta(commitMeta);

      String lastHash = hashesToTransplant.get(hashesToTransplant.size() - 1);
      ResolvedHash fromRef =
          getHashResolver()
              .resolveHashOnRef(
                  fromRefName,
                  lastHash,
                  new HashValidator("Hash to transplant").hashMustBeUnambiguous());

      ResolvedHash toRef =
          getHashResolver()
              .resolveHashOnRef(
                  branchName,
                  expectedHash,
                  new HashValidator("Reference to transplant into", "Expected hash")
                      .refMustBeBranch()
                      .hashMustBeUnambiguous());

      startAccessCheck()
          .canViewReference(fromRef.getNamedRef())
          .canCommitChangeAgainstReference(toRef.getNamedRef())
          .checkAndThrow();

      List<Hash> transplants = new ArrayList<>(hashesToTransplant.size());
      for (String hash : hashesToTransplant) {
        transplants.add(
            getHashResolver()
                .resolveHashOnRef(
                    fromRef, hash, new HashValidator("Hash to transplant").hashMustBeUnambiguous())
                .getHash());
      }

      if (transplants.size() > 1) {
        // Message overrides are not meaningful when transplanting more than one commit.
        // This matches old behaviour where `message` was ignored in all cases.
        commitMeta = null;
      }

      TransplantResult result =
          getStore()
              .transplant(
                  TransplantOp.builder()
                      .fromRef(fromRef.getNamedRef())
                      .toBranch((BranchName) toRef.getNamedRef())
                      .expectedHash(Optional.of(toRef.getHash()))
                      .sequenceToTransplant(transplants)
                      .updateCommitMetadata(
                          commitMetaUpdate(
                              commitMeta,
                              numCommits ->
                                  String.format(
                                      "Transplanted %d commits from %s at %s into %s at %s",
                                      numCommits,
                                      fromRefName,
                                      lastHash,
                                      branchName,
                                      toRef.getHash().asString())))
                      .mergeKeyBehaviors(keyMergeBehaviors(keyMergeBehaviors))
                      .defaultMergeBehavior(defaultMergeBehavior(defaultMergeBehavior))
                      .dryRun(Boolean.TRUE.equals(dryRun))
                      .fetchAdditionalInfo(Boolean.TRUE.equals(fetchAdditionalInfo))
                      .validator(createCommitValidator((BranchName) toRef.getNamedRef(), API_WRITE))
                      .build());
      return createResponse(result);
    } catch (ReferenceNotFoundException e) {
      throw new NessieReferenceNotFoundException(e.getMessage(), e);
    } catch (MergeConflictException e) {
      if (Boolean.TRUE.equals(returnConflictAsResult)) {
        return createResponse(e.getMergeResult());
      }
      throw new NessieReferenceConflictException(e.getReferenceConflicts(), e.getMessage(), e);
    } catch (ReferenceConflictException e) {
      throw new NessieReferenceConflictException(e.getReferenceConflicts(), e.getMessage(), e);
    }
  }

  @Override
  public MergeResponse mergeRefIntoBranch(
      String branchName,
      String expectedHash,
      String fromRefName,
      String fromHash,
      @Nullable CommitMeta commitMeta,
      Collection<MergeKeyBehavior> keyMergeBehaviors,
      MergeBehavior defaultMergeBehavior,
      Boolean dryRun,
      Boolean fetchAdditionalInfo,
      Boolean returnConflictAsResult)
      throws NessieNotFoundException, NessieConflictException {
    try {
      validateCommitMeta(commitMeta);

      ResolvedHash fromRef =
          getHashResolver()
              .resolveHashOnRef(
                  fromRefName, fromHash, new HashValidator("Source hash").hashMustBeUnambiguous());
      ResolvedHash toRef =
          getHashResolver()
              .resolveHashOnRef(
                  branchName,
                  expectedHash,
                  new HashValidator("Reference to merge into", "Expected hash")
                      .refMustBeBranch()
                      .hashMustBeUnambiguous());

      checkArgument(toRef.getNamedRef() instanceof BranchName, "Can only merge into branches.");

      startAccessCheck()
          .canViewReference(fromRef.getNamedRef())
          .canCommitChangeAgainstReference(toRef.getNamedRef())
          .checkAndThrow();

      MergeResult result =
          getStore()
              .merge(
                  MergeOp.builder()
                      .fromRef(fromRef.getNamedRef())
                      .fromHash(fromRef.getHash())
                      .toBranch((BranchName) toRef.getNamedRef())
                      .expectedHash(Optional.of(toRef.getHash()))
                      .updateCommitMetadata(
                          commitMetaUpdate(
                              commitMeta,
                              numCommits ->
                                  // numCommits is always 1 for merges
                                  String.format(
                                      "Merged %s at %s into %s at %s",
                                      fromRefName,
                                      fromRef.getHash().asString(),
                                      branchName,
                                      toRef.getHash().asString())))
                      .mergeKeyBehaviors(keyMergeBehaviors(keyMergeBehaviors))
                      .defaultMergeBehavior(defaultMergeBehavior(defaultMergeBehavior))
                      .dryRun(Boolean.TRUE.equals(dryRun))
                      .fetchAdditionalInfo(Boolean.TRUE.equals(fetchAdditionalInfo))
                      .validator(createCommitValidator((BranchName) toRef.getNamedRef(), API_WRITE))
                      .build());
      return createResponse(result);
    } catch (ReferenceNotFoundException e) {
      throw new NessieReferenceNotFoundException(e.getMessage(), e);
    } catch (MergeConflictException e) {
      if (Boolean.TRUE.equals(returnConflictAsResult)) {
        return createResponse(e.getMergeResult());
      }
      throw new NessieReferenceConflictException(e.getReferenceConflicts(), e.getMessage(), e);
    } catch (ReferenceConflictException e) {
      throw new NessieReferenceConflictException(e.getReferenceConflicts(), e.getMessage(), e);
    }
  }

  private static void validateCommitMeta(CommitMeta commitMeta) {
    if (commitMeta != null) {
      checkArgument(
          commitMeta.getCommitter() == null,
          "Cannot set the committer on the client side. It is set by the server.");
      checkArgument(
          commitMeta.getCommitTime() == null,
          "Cannot set the commit time on the client side. It is set by the server.");
      checkArgument(
          commitMeta.getHash() == null,
          "Cannot set the commit hash on the client side. It is set by the server.");
      checkArgument(
          commitMeta.getParentCommitHashes() == null
              || commitMeta.getParentCommitHashes().isEmpty(),
          "Cannot set the parent commit hashes on the client side. It is set by the server.");
    }
  }

  @SuppressWarnings("deprecation")
  private MergeResponse createResponse(MergeTransplantResultBase result) {
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

    result
        .getDetails()
        .forEach(
            (key, details) -> {
              ImmutableContentKeyDetails.Builder keyDetails =
                  ImmutableContentKeyDetails.builder()
                      .key(ContentKey.of(key.getElements()))
                      .conflictType(
                          details.getConflict() != null
                              ? ContentKeyConflict.UNRESOLVABLE
                              : ContentKeyConflict.NONE)
                      .mergeBehavior(MergeBehavior.valueOf(details.getMergeBehavior().name()))
                      .conflict(details.getConflict());

              response.addDetails(keyDetails.build());
            });

    return response.build();
  }

  private static Map<ContentKey, MergeKeyBehavior> keyMergeBehaviors(
      Collection<MergeKeyBehavior> behaviors) {
    return behaviors != null
        ? behaviors.stream().collect(Collectors.toMap(MergeKeyBehavior::getKey, identity()))
        : Collections.emptyMap();
  }

  private static MergeBehavior defaultMergeBehavior(MergeBehavior mergeBehavior) {
    return mergeBehavior != null ? mergeBehavior : MergeBehavior.NORMAL;
  }

  @Override
  public <R> R getEntries(
      String namedRef,
      String hashOnRef,
      Integer namespaceDepth,
      String filter,
      String pagingToken,
      boolean withContent,
      PagedResponseHandler<R, Entry> pagedResponseHandler,
      Consumer<WithHash<NamedRef>> effectiveReference,
      ContentKey minKey,
      ContentKey maxKey,
      ContentKey prefixKey,
      List<ContentKey> requestedKeys)
      throws NessieNotFoundException {

    try {
      ResolvedHash refWithHash =
          getHashResolver()
              .resolveHashOnRef(namedRef, hashOnRef, new HashValidator("Expected hash"));

      effectiveReference.accept(refWithHash);
      BiPredicate<ContentKey, Content.Type> contentKeyPredicate = null;
      if (requestedKeys != null && !requestedKeys.isEmpty()) {
        Set<ContentKey> requestedKeysSet = new HashSet<>(requestedKeys);
        contentKeyPredicate = (key, type) -> requestedKeysSet.contains(key);
        if (prefixKey == null) {
          // Populate minKey/maxKey if not set or if those would query "more" keys.
          ContentKey minRequested = null;
          ContentKey maxRequested = null;
          for (ContentKey requestedKey : requestedKeys) {
            if (minRequested == null || requestedKey.compareTo(minRequested) < 0) {
              minRequested = requestedKey;
            }
            if (maxRequested == null || requestedKey.compareTo(maxRequested) > 0) {
              maxRequested = requestedKey;
            }
          }
          if (minKey == null || minKey.compareTo(minRequested) < 0) {
            minKey = minRequested;
          }
          if (maxKey == null || maxKey.compareTo(maxRequested) > 0) {
            maxKey = maxRequested;
          }
        }
      }

      // apply filter as early as possible to avoid work (i.e. content loading, authz checks)
      // for entries that we will eventually throw away
      final int namespaceFilterDepth = namespaceDepth == null ? 0 : namespaceDepth;
      if (namespaceFilterDepth > 0) {
        BiPredicate<ContentKey, Content.Type> depthFilter =
            (key, type) -> key.getElementCount() >= namespaceFilterDepth;
        contentKeyPredicate = combinePredicateWithAnd(contentKeyPredicate, depthFilter);
      }
      BiPredicate<ContentKey, Content.Type> filterPredicate = filterEntries(filter);
      contentKeyPredicate = combinePredicateWithAnd(contentKeyPredicate, filterPredicate);

      try (PaginationIterator<KeyEntry> entries =
          getStore()
              .getKeys(
                  refWithHash.getHash(),
                  pagingToken,
                  withContent,
                  VersionStore.KeyRestrictions.builder()
                      .minKey(minKey)
                      .maxKey(maxKey)
                      .prefixKey(prefixKey)
                      .contentKeyPredicate(contentKeyPredicate)
                      .build())) {

        AuthzPaginationIterator<KeyEntry> authz =
            new AuthzPaginationIterator<KeyEntry>(
                entries, super::startAccessCheck, getServerConfig().accessChecksBatchSize()) {
              @Override
              protected Set<Check> checksForEntry(KeyEntry entry) {
                return singleton(canReadContentKey(refWithHash.getValue(), entry.getKey()));
              }
            }.initialCheck(canReadEntries(refWithHash.getValue()));

        Set<ContentKey> seenNamespaces = namespaceFilterDepth > 0 ? new HashSet<>() : null;
        while (authz.hasNext()) {
          KeyEntry key = authz.next();

          Content c = key.getContent();
          Entry entry =
              c != null
                  ? Entry.entry(key.getKey().contentKey(), key.getKey().type(), c)
                  : Entry.entry(
                      key.getKey().contentKey(),
                      key.getKey().type(),
                      key.getKey().lastElement().contentId());

          if (namespaceFilterDepth > 0) {
            entry = maybeTruncateToDepth(entry, namespaceFilterDepth);
          }

          // add implicit namespace entries only once (single parent of multiple real entries)
          if (seenNamespaces == null
              || !Content.Type.NAMESPACE.equals(entry.getType())
              || seenNamespaces.add(entry.getName())) {
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

  private static Entry maybeTruncateToDepth(Entry entry, int depth) {
    List<String> nameElements = entry.getName().getElements();
    boolean truncateToNamespace = nameElements.size() > depth;
    if (truncateToNamespace) {
      // implicit namespace entry at target depth (virtual parent of real entry)
      ContentKey namespaceKey = ContentKey.of(nameElements.subList(0, depth));
      return Entry.entry(namespaceKey, Content.Type.NAMESPACE);
    }
    return entry;
  }

  private static BiPredicate<ContentKey, Content.Type> combinePredicateWithAnd(
      BiPredicate<ContentKey, Content.Type> a, BiPredicate<ContentKey, Content.Type> b) {
    if (a == null) {
      return b;
    }
    if (b == null) {
      return a;
    }
    return a.and(b);
  }

  /**
   * Produces the predicate for key-entry filtering.
   *
   * @param filter The filter to filter by
   */
  protected BiPredicate<ContentKey, Content.Type> filterEntries(String filter) {
    if (Strings.isNullOrEmpty(filter)) {
      return null;
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
    return (key, type) -> {
      Map<String, Object> arguments = ImmutableMap.of(VAR_ENTRY, CELUtil.forCel(key, type));

      try {
        return script.execute(Boolean.class, arguments);
      } catch (ScriptException e) {
        throw new RuntimeException(e);
      }
    };
  }

  @Override
  public CommitResponse commitMultipleOperations(
      String branch, String expectedHash, Operations operations, RequestMeta requestMeta)
      throws NessieNotFoundException, NessieConflictException {

    CommitMeta commitMeta = operations.getCommitMeta();
    validateCommitMeta(commitMeta);

    try {
      ImmutableCommitResponse.Builder commitResponse = ImmutableCommitResponse.builder();

      ResolvedHash toRef =
          getHashResolver()
              .resolveHashOnRef(
                  branch,
                  expectedHash,
                  new HashValidator("Reference to commit into", "Expected hash")
                      .refMustBeBranch()
                      .hashMustBeUnambiguous());

      Hash newHash =
          getStore()
              .commit(
                  (BranchName) toRef.getNamedRef(),
                  Optional.of(toRef.getHash()),
                  commitMetaUpdate(null, numCommits -> null).rewriteSingle(commitMeta),
                  operations.getOperations(),
                  createCommitValidator((BranchName) toRef.getNamedRef(), requestMeta),
                  (key, cid) -> commitResponse.addAddedContents(addedContent(key, cid)))
              .getCommitHash();

      return commitResponse.targetBranch(Branch.of(branch, newHash.asString())).build();
    } catch (ReferenceNotFoundException e) {
      throw new NessieReferenceNotFoundException(e.getMessage(), e);
    } catch (ReferenceConflictException e) {
      throw new NessieReferenceConflictException(e.getReferenceConflicts(), e.getMessage(), e);
    }
  }

  private CommitValidator createCommitValidator(BranchName branchName, RequestMeta requestMeta) {
    // Commits routinely run retries due to collisions on updating the HEAD of the branch.
    // Authorization is not dependent on the commit history, only on the collection of access
    // checks, which reflect the current commit. On retries, the commit data relevant to access
    // checks almost never changes. Therefore, we use RetriableAccessChecker to avoid re-validating
    // access checks (which could be a time-consuming operation) on subsequent retries, unless
    // authorization input data changes.
    RetriableAccessChecker accessChecker =
        new RetriableAccessChecker(this::startAccessCheck, getApiContext());
    return validation -> {
      BatchAccessChecker check = accessChecker.newAttempt();
      check.canCommitChangeAgainstReference(branchName);
      validation
          .operations()
          .forEach(
              op -> {
                Set<String> keyActions = requestMeta.keyActions(op.identifiedKey().contentKey());
                switch (op.operationType()) {
                  case CREATE:
                    check.canCreateEntity(branchName, op.identifiedKey(), keyActions);
                    break;
                  case UPDATE:
                    check.canUpdateEntity(branchName, op.identifiedKey(), keyActions);
                    break;
                  case DELETE:
                    check.canDeleteEntity(branchName, op.identifiedKey(), keyActions);
                    break;
                  default:
                    throw new UnsupportedOperationException(
                        "Unknown operation type " + op.operationType());
                }
              });
      check.checkAndThrow();
    };
  }

  private static Reference makeReference(
      ReferenceInfo<CommitMeta> refWithHash, boolean fetchMetadata) {
    NamedRef ref = refWithHash.getNamedRef();
    if (ref instanceof TagName) {
      return Tag.of(
          ref.getName(),
          refWithHash.getHash().asString(),
          fetchMetadata ? extractReferenceMetadata(refWithHash) : null);
    } else if (ref instanceof BranchName) {
      return Branch.of(
          ref.getName(),
          refWithHash.getHash().asString(),
          fetchMetadata ? extractReferenceMetadata(refWithHash) : null);
    } else {
      throw new UnsupportedOperationException("only converting tags or branches");
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
      builder.commitMetaOfHEAD(refWithHash.getHeadCommitMeta());
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
}

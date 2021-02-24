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
package org.projectnessie.versioned.jgit;

import static java.lang.String.format;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import javax.annotation.Nonnull;
import javax.inject.Inject;

import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.LogCommand;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.eclipse.jgit.errors.AmbiguousObjectException;
import org.eclipse.jgit.errors.ConfigInvalidException;
import org.eclipse.jgit.errors.IncorrectObjectTypeException;
import org.eclipse.jgit.errors.InvalidObjectIdException;
import org.eclipse.jgit.lib.CommitBuilder;
import org.eclipse.jgit.lib.Constants;
import org.eclipse.jgit.lib.ObjectId;
import org.eclipse.jgit.lib.ObjectInserter;
import org.eclipse.jgit.lib.ObjectLoader;
import org.eclipse.jgit.lib.PersonIdent;
import org.eclipse.jgit.lib.RefUpdate;
import org.eclipse.jgit.lib.RefUpdate.Result;
import org.eclipse.jgit.lib.Repository;
import org.eclipse.jgit.lib.TreeFormatter;
import org.eclipse.jgit.lib.UserConfig;
import org.eclipse.jgit.revwalk.RevCommit;
import org.eclipse.jgit.revwalk.RevWalk;
import org.eclipse.jgit.treewalk.TreeWalk;
import org.eclipse.jgit.treewalk.filter.PathFilter;
import org.eclipse.jgit.util.SystemReader;
import org.projectnessie.versioned.BranchName;
import org.projectnessie.versioned.Diff;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.Key;
import org.projectnessie.versioned.NamedRef;
import org.projectnessie.versioned.Operation;
import org.projectnessie.versioned.Ref;
import org.projectnessie.versioned.ReferenceAlreadyExistsException;
import org.projectnessie.versioned.ReferenceConflictException;
import org.projectnessie.versioned.ReferenceNotFoundException;
import org.projectnessie.versioned.Serializer;
import org.projectnessie.versioned.StoreWorker;
import org.projectnessie.versioned.TagName;
import org.projectnessie.versioned.Unchanged;
import org.projectnessie.versioned.VersionStore;
import org.projectnessie.versioned.WithEntityType;
import org.projectnessie.versioned.WithHash;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;

/**
 * VersionStore interface for JGit backend.
 */
public class JGitVersionStore<TABLE, METADATA> implements VersionStore<TABLE, METADATA> {

  private static final Logger logger = LoggerFactory.getLogger(JGitVersionStore.class);
  private static final String SLASH = "/";

  private final Repository repository;
  private final StoreWorker<TABLE, METADATA> storeWorker;
  private final ObjectId emptyObject;

  /**
   * Construct a JGitVersionStore.
   */
  @Inject
  public JGitVersionStore(Repository repository, StoreWorker<TABLE, METADATA> storeWorker) {
    this.storeWorker = storeWorker;
    this.repository = repository;
    ObjectId objectId;
    try {
      ObjectInserter oi = repository.newObjectInserter();
      objectId = oi.insert(Constants.OBJ_BLOB, new byte[] {0});
      oi.flush();
    } catch (IOException e) {
      objectId = null;
      logger.warn("Unable to insert empty object which is used as a sentinel for deletes. "
                  + "This is likely safe to ignore but could indicate a larger problem with the repository.", e);
    }
    emptyObject = objectId;
  }

  @Nonnull
  @Override
  public Hash toHash(@Nonnull NamedRef ref) throws ReferenceNotFoundException {
    repository.getRefDatabase().refresh();
    final org.eclipse.jgit.lib.Ref jgitRef;
    try {
      if (ref instanceof BranchName) {
        jgitRef = repository.findRef(Constants.R_HEADS + ref.getName());
      } else if (ref instanceof TagName) {
        jgitRef = repository.findRef(Constants.R_TAGS + ref.getName());
      } else {
        throw new IllegalStateException(String.format("ref %s is not in allowed types", ref));
      }
    } catch (IOException e) {
      throw new RuntimeException("Error talking to git repo", e);
    }
    if (jgitRef == null) {
      throw new ReferenceNotFoundException(String.format("Ref %s was not found in the git database", ref));
    }
    return Hash.of(jgitRef.getObjectId().name());
  }

  @Override
  public WithHash<Ref> toRef(String refOfUnknownType) throws ReferenceNotFoundException {
    try {
      org.eclipse.jgit.lib.Ref jgitRef;

      jgitRef = repository.findRef(Constants.R_HEADS + refOfUnknownType);

      // branch first.
      if (jgitRef != null) {
        return WithHash.of(Hash.of(jgitRef.getObjectId().name()), BranchName.of(refOfUnknownType));
      }

      // then tag.
      jgitRef = repository.findRef(Constants.R_TAGS + refOfUnknownType);
      if (jgitRef != null) {
        return WithHash.of(Hash.of(jgitRef.getObjectId().name()), TagName.of(refOfUnknownType));
      }

      // hash last.
      try {
        ObjectId hash = repository.resolve(refOfUnknownType + "^{tree}");
        if (hash == null) {
          throw ReferenceNotFoundException.forReference(refOfUnknownType);
        }
        return WithHash.of(Hash.of(refOfUnknownType), Hash.of(refOfUnknownType));
      } catch (IllegalArgumentException | AmbiguousObjectException | IncorrectObjectTypeException e) {
        throw new ReferenceNotFoundException(String.format("Unable to find the requested reference %s.", refOfUnknownType));
      }
    } catch (IOException e) {
      throw new RuntimeException("Error talking to git repo", e);
    }
  }

  private void testExpectedHash(BranchName branch, Optional<Hash> expectedHash) throws ReferenceNotFoundException {
    if (expectedHash.isPresent()) {
      try {
        testLinearTransplantList(ImmutableList.of(expectedHash.get(), toHash(branch)));
      } catch (IllegalArgumentException e) {
        throw ReferenceNotFoundException.forReference(expectedHash.get());
      }
    }
  }

  @Override
  public void commit(BranchName branch, Optional<Hash> expectedHash, METADATA metadata,
                     List<Operation<WithEntityType<TABLE>>> operations) throws ReferenceNotFoundException, ReferenceConflictException {
    toHash(branch);
    try {
      testExpectedHash(branch, expectedHash);
      ObjectId commits = TreeBuilder.commitObjects(operations, repository, storeWorker.getValueWorker(), emptyObject);
      ObjectId treeId = repository.resolve(expectedHash.map(Hash::asString).orElse(branch.getName()) + "^{tree}");
      if (treeId == null) {
        throw ReferenceNotFoundException.forReference(expectedHash.map(x -> (Ref) x).orElse(branch));
      }
      ObjectId newTree = TreeBuilder.merge(treeId, commits, repository);

      List<String> unchanged = operations.stream()
                                         .filter(e -> e instanceof Unchanged)
                                         .map(Operation::getKey)
                                         .map(JGitVersionStore::stringFromKey)
                                         .collect(Collectors.toList());
      Optional<ObjectId> mergedTree = commitTreeWithTwoWayMerge(branch, expectedHash, newTree, unchanged);
      ObjectId currentCommitId = repository.resolve(branch.getName() + "^{commit}");
      ObjectId currentTreeId = repository.resolve(branch.getName() + "^{tree}");
      ObjectId mergedHash = mergedTree.orElseThrow(() -> ReferenceConflictException.forReference(branch,
                                                                                                 expectedHash,
                                                                                                 Optional.of(currentCommitId)
                                                                                                         .map(ObjectId::name)
                                                                                                         .map(Hash::of)));
      commitTree(branch,
                 mergedHash,
                 Optional.of(currentCommitId).map(ObjectId::name).map(Hash::of),
                 metadata,
                 ObjectId.isEqual(currentTreeId, mergedHash),
                 false);
    } catch (IOException e) {
      throw new RuntimeException("Unknown error", e);
    }
  }

  private Optional<ObjectId> commitTreeWithTwoWayMerge(BranchName branch, Optional<Hash> expectedHash, ObjectId newTree,
                                                       List<String> unchanged) throws IOException {
    ObjectId currentTreeId = repository.resolve(branch.getName() + "^{tree}");
    ObjectId expectedId = repository.resolve(expectedHash.map(Hash::asString).orElse(currentTreeId.name()) + "^{tree}");
    Optional<ObjectId> mergedTree = tryTwoWayMerge(currentTreeId,
                                                   newTree,
                                                   repository.newObjectInserter(),
                                                   expectedId,
                                                   unchanged);

    return mergedTree;
  }

  private void testLinearTransplantList(List<Hash> sequenceToTransplant) throws ReferenceNotFoundException {
    try (RevWalk rw = new RevWalk(repository)) {
      RevCommit start = null;
      for (Hash hash : sequenceToTransplant) {
        RevCommit commit;
        try {
          ObjectId obj = repository.resolve(hash.asString() + "^{commit}");
          commit = rw.parseCommit(obj);
        } catch (IOException | NullPointerException e) {
          throw ReferenceNotFoundException.forReference(hash);
        }
        if (start == null) {
          start = commit;
          continue;
        }
        try {
          if (!rw.isMergedInto(start, commit)) {
            throw new IllegalArgumentException(format("Hash %s is not the ancestor for commit %s", start, hash));
          } else {
            start = commit;
          }
        } catch (IOException e) {
          throw new IllegalArgumentException(format("Hash %s is not the ancestor for commit %s", start, hash));
        }
      }
    }
  }

  @Override
  public void transplant(BranchName targetBranch, Optional<Hash> expectedHash,
                         List<Hash> sequenceToTransplant) throws ReferenceNotFoundException, ReferenceConflictException {
    testLinearTransplantList(sequenceToTransplant);
    try {
      ObjectId targetTreeId = repository.resolve(targetBranch.getName() + "^{tree}");
      if (targetTreeId == null) {
        throw ReferenceNotFoundException.forReference(expectedHash.map(x -> (Ref) x).orElse(targetBranch));
      }
      testExpectedHash(targetBranch, expectedHash);
      ObjectId newTree = null;
      for (Hash hash: sequenceToTransplant) {
        ObjectId transplantTree = TreeBuilder.transplant(hash, repository);
        if (newTree == null) {
          newTree = transplantTree;
        } else {
          newTree = TreeBuilder.merge(newTree, transplantTree, repository);
        }
      }
      Optional<ObjectId> mergeTree = commitTreeWithTwoWayMerge(targetBranch, expectedHash, newTree, Collections.emptyList());
      ObjectId currentCommitId = repository.resolve(targetBranch.getName() + "^{commit}");
      ObjectId currentTreeId = repository.resolve(targetBranch.getName() + "^{tree}");
      if (!mergeTree.isPresent()) {
        throw ReferenceConflictException.forReference(targetBranch,
                                                      expectedHash,
                                                      Optional.of(currentCommitId).map(ObjectId::name).map(Hash::of));
      }
      for (Hash hash: sequenceToTransplant) {
        ObjectId transplantTree = TreeBuilder.merge(currentTreeId, TreeBuilder.transplant(hash, repository), repository);
        commitTree(targetBranch,
                   transplantTree,
                   Optional.of(currentCommitId).map(ObjectId::name).map(Hash::of),
                   getCommit(hash),
                   false,
                   false);
        currentCommitId = repository.resolve(targetBranch.getName() + "^{commit}");
        currentTreeId = repository.resolve(targetBranch.getName() + "^{tree}");
      }
    } catch (IOException e) {
      throw new RuntimeException("Unknown error", e);
    }
  }

  @Override
  public void merge(Hash fromHash, BranchName toBranch, Optional<Hash> expectedHash)
      throws ReferenceNotFoundException, ReferenceConflictException {
    try {
      org.eclipse.jgit.lib.Ref ref = repository.findRef(Constants.R_HEADS + toBranch.getName());
      if (ref == null) {
        throw ReferenceNotFoundException.forReference(expectedHash.map(x -> (Ref) x).orElse(toBranch));
      }
      ObjectId newCommitId = repository.resolve(fromHash.asString() + "^{commit}");
      if (newCommitId == null) {
        throw ReferenceNotFoundException.forReference(fromHash);
      }
      RevCommit newCommit = RevCommit.parse(repository.getObjectDatabase().open(newCommitId).getBytes());
      try (RevWalk walk = new RevWalk(repository)) {
        ObjectId headId = ref.getObjectId();
        String headName = ref.getName();
        RevCommit headCommit = walk.lookupCommit(headId);
        RevCommit upstream = walk.lookupCommit(newCommit.getId());

        if (walk.isMergedInto(upstream, headCommit)) {
          return;
        } else if (walk.isMergedInto(headCommit, upstream)) {
          RefUpdate rup = repository.updateRef(headName);
          rup.setNewObjectId(newCommit);
          expectedHash.map(Hash::asString).map(ObjectId::fromString).ifPresent(rup::setExpectedOldObjectId);
          Result res = rup.forceUpdate();
          switch (res) {
            case FAST_FORWARD:
            case FORCED:
            case NO_CHANGE:
              return;
            default:
              throw new IOException("failed update");
          }
        }
        List<RevCommit> pickList = calculatePickList(newCommit, headCommit);
        transplant(toBranch, expectedHash, pickList.stream().map(RevCommit::name).map(Hash::of).collect(Collectors.toList()));
      }
    } catch (IOException e) {
      throw new RuntimeException("Unknown error", e);
    }
  }

  private List<RevCommit> calculatePickList(RevCommit headCommit, RevCommit upstreamCommit)
      throws IOException {
    Iterable<RevCommit> commitsToUse;
    try (Git git = new Git(repository)) {
      LogCommand cmd = git.log().addRange(upstreamCommit, headCommit);
      commitsToUse = cmd.call();
    } catch (GitAPIException e) {
      throw new IOException(e);
    }
    List<RevCommit> cherryPickList = new ArrayList<>();
    for (RevCommit commit : commitsToUse) {
      if (commit.getParentCount() == 1) {
        cherryPickList.add(commit);
      }
    }
    Collections.reverse(cherryPickList);

    return cherryPickList;
  }

  @Override
  public void assign(NamedRef ref, Optional<Hash> expectedHash, Hash targetHash)
      throws ReferenceNotFoundException, ReferenceConflictException {

    Hash existingHash = toHash(ref);
    if (expectedHash.isPresent() && !existingHash.equals(expectedHash.get())) {
      throw new ReferenceConflictException(String.format("expected hash %s does not match current hash %s", expectedHash, existingHash));
    }

    try {
      updateRef(ref, targetHash, expectedHash, true);
    } catch (IOException e) {
      throw new RuntimeException("Unknown error", e);
    }
  }


  @Override
  public void create(NamedRef ref, Optional<Hash> targetHash) throws ReferenceNotFoundException, ReferenceAlreadyExistsException {
    if (!targetHash.isPresent() && ref instanceof TagName) {
      throw new IllegalArgumentException("You must provide a target hash to create a tag.");
    }
    try {
      toHash(ref);
      throw new ReferenceAlreadyExistsException(String.format("ref %s already exists", ref));
    } catch (ReferenceNotFoundException e) {
      //pass expected
    }
    try {
      if (!targetHash.isPresent()) {
        TreeFormatter formatter = new TreeFormatter();
        ObjectInserter inserter = repository.newObjectInserter();
        ObjectId newTreeId = inserter.insert(formatter);
        inserter.flush();
        commitTree((BranchName) ref, newTreeId, Optional.empty(), null, false, true);
      } else {
        ObjectId target = repository.resolve(targetHash.get().asString());
        RefUpdate createBranch = repository.updateRef((ref instanceof TagName ? Constants.R_TAGS : Constants.R_HEADS) + ref.getName());
        createBranch.setNewObjectId(target);
        Result result = createBranch.update();
        if (result.equals(Result.REJECTED_MISSING_OBJECT)) {
          throw ReferenceNotFoundException.forReference(targetHash.get());
        } else if (!result.equals(Result.NEW)) {
          throw new IllegalStateException(String.format("result did not complete for create branch on %s with state %s", ref, result));
        }
      }
    } catch (IOException | ReferenceConflictException e) {
      throw new RuntimeException(String.format("Unknown error while creating %s", ref), e);
    }
  }

  @Override
  public void delete(NamedRef ref, Optional<Hash> hash) throws ReferenceNotFoundException, ReferenceConflictException {
    toHash(ref);
    try {
      RefUpdate update = repository.updateRef((ref instanceof TagName ? Constants.R_TAGS : Constants.R_HEADS) + ref.getName());
      Optional<ObjectId> objectId = fromHash(ref, hash);
      if (objectId.isPresent() && !ObjectId.isEqual(update.getRef().getObjectId(), objectId.get())) {
        throw ReferenceConflictException.forReference(ref, hash, Optional.empty());
      }
      update.setForceUpdate(true);
      objectId.ifPresent(update::setExpectedOldObjectId);
      Result deleteResult = update.delete();
      if (deleteResult.equals(Result.REJECTED_MISSING_OBJECT)) {
        throw ReferenceNotFoundException.forReference(hash.get());
      } else if (!deleteResult.equals(Result.FORCED)) {
        throw ReferenceConflictException.forReference(ref, hash, Optional.empty());
      }
    } catch (IOException e) {
      throw new RuntimeException("Unknown error", e);
    }
  }

  @Override
  public Stream<WithHash<NamedRef>> getNamedRefs() {
    try {
      Stream<WithHash<NamedRef>> branches = repository.getRefDatabase()
                                                      .getRefsByPrefix(Constants.R_HEADS)
                                                      .stream()
                                                      .map(r -> WithHash.of(Hash.of(r.getObjectId().name()),
                                                                            BranchName.of(r.getName().replace(Constants.R_HEADS, ""))));
      Stream<WithHash<NamedRef>> tags = repository.getRefDatabase()
                                                  .getRefsByPrefix(Constants.R_TAGS)
                                                  .stream()
                                                  .map(r -> WithHash.of(Hash.of(r.getObjectId().name()),
                                                                        TagName.of(r.getName().replace(Constants.R_TAGS, ""))));
      return Stream.concat(branches, tags);
    } catch (IOException e) {
      throw new RuntimeException("Unknown error", e);
    }
  }

  @Override
  public Stream<WithHash<METADATA>> getCommits(Ref ref) throws ReferenceNotFoundException {
    final String hashName = refName(ref);
    try {
      ObjectId objectId = repository.resolve(hashName);
      if (objectId == null) {
        throw new ReferenceNotFoundException(String.format("Ref %s not found", ref));
      }
      RevWalk walk = new RevWalk(repository);
      walk.markStart(repository.parseCommit(objectId));
      Serializer<METADATA> serializer = storeWorker.getMetadataSerializer();
      //note: skipLastElement doesn't return the absolute base commit. This is because other version stores don't consider that a commit.
      return StreamSupport.stream(skipLastElement(walk.spliterator()), false)
                          .map(r -> {
                            Hash hash = Hash.of(r.name());
                            METADATA metadata = serializer.fromBytes(ByteString.copyFrom(r.getFullMessage(), StandardCharsets.UTF_8));
                            return WithHash.of(hash, metadata);
                          });
    } catch (IOException e) {
      throw new RuntimeException("Unknown error", e);
    }
  }

  private METADATA getCommit(Hash hash) throws IOException {
    RevCommit r = repository.parseCommit(ObjectId.fromString(hash.asString()));
    Serializer<METADATA> serializer = storeWorker.getMetadataSerializer();
    METADATA metadata = serializer.fromBytes(ByteString.copyFrom(r.getFullMessage(), StandardCharsets.UTF_8));
    return metadata;
  }

  @Override
  public Stream<WithEntityType<Key>> getKeys(Ref ref) {
    try {
      List<WithEntityType<Key>> tables = new ArrayList<>();
      try (TreeWalk treeWalk = new TreeWalk(repository)) {
        ObjectId treeId = repository.resolve(refName(ref) + "^{tree}");
        treeWalk.addTree(treeId);
        treeWalk.setRecursive(true);
        while (treeWalk.next()) {
          byte[] bytes = getTable(treeWalk, repository);
          byte type = storeWorker.getValueWorker().fromBytes(ByteString.copyFrom(bytes)).getEntityType();
          tables.add(WithEntityType.of(type, keyFromUrlString(treeWalk.getPathString())));

        }
      }
      return tables.stream();
    } catch (IOException e) {
      throw new RuntimeException("Unknown error", e);
    }
  }

  @Override
  public WithEntityType<TABLE> getValue(Ref ref, Key key) throws ReferenceNotFoundException {
    final String hashName = refName(ref);
    String table = stringFromKey(key);
    try {
      try (TreeWalk treeWalk = new TreeWalk(repository)) {
        ObjectId treeId = repository.resolve(hashName + "^{tree}");
        treeWalk.addTree(treeId);
        treeWalk.setRecursive(true);
        treeWalk.setFilter(PathFilter.create(table));
        while (treeWalk.next()) {
          byte[] bytes = getTable(treeWalk, repository);
          return storeWorker.getValueWorker().fromBytes(ByteString.copyFrom(bytes));
        }
      }
      return null;
    } catch (IOException e) {
      throw new ReferenceNotFoundException(String.format("reference for ref %s and key %s not found", ref, key), e);
    }
  }

  @Override
  public List<Optional<WithEntityType<TABLE>>> getValues(Ref ref, List<Key> key) {
    final String hashName = refName(ref);
    Map<String, Key> keys = key.stream().collect(Collectors.toMap(JGitVersionStore::stringFromKey, k -> k));
    Map<Key, WithEntityType<TABLE>> tables = new HashMap<>();
    try {
      try (TreeWalk treeWalk = new TreeWalk(repository)) {
        ObjectId treeId = repository.resolve(hashName + "^{tree}");
        treeWalk.addTree(treeId);
        treeWalk.setRecursive(true);
        while (treeWalk.next()) {
          if (keys.containsKey(treeWalk.getPathString())) {
            byte[] bytes = getTable(treeWalk, repository);
            tables.put(keys.get(treeWalk.getPathString()), storeWorker.getValueWorker().fromBytes(ByteString.copyFrom(bytes)));
          }
        }
      }
    } catch (IOException e) {
      throw new RuntimeException("Unknown jgit error", e);
    }
    return key.stream().map(k -> Optional.ofNullable(tables.get(k))).collect(Collectors.toList());
  }

  @Override
  public Collector collectGarbage() {
    throw new IllegalStateException("Not yet implemented.");
  }

  private void commitTree(BranchName branch, ObjectId newTree, Optional<Hash> expectedHash, METADATA metadata, boolean force, boolean empty)
      throws IOException, ReferenceConflictException {
    ObjectInserter inserter = repository.newObjectInserter();
    CommitBuilder commitBuilder = fromUser(metadata, empty);
    commitBuilder.setTreeId(newTree);
    ObjectId parentId = fromHash(branch, expectedHash).orElse(repository.resolve(Constants.R_HEADS + branch.getName()));
    if (parentId != null) {
      commitBuilder.setParentId(parentId);
    }
    ObjectId newCommitId = inserter.insert(commitBuilder);
    inserter.flush();
    updateRef(branch, newCommitId, expectedHash, force);
  }

  private void updateRef(NamedRef ref, Hash targetHash, Optional<Hash> expectedHash, boolean force)
      throws IOException, ReferenceConflictException, ReferenceNotFoundException {
    ObjectId target = repository.resolve(targetHash.asString());
    if (target == null) {
      throw ReferenceNotFoundException.forReference(ref);
    }
    updateRef(ref, target, expectedHash, force);
  }

  private void updateRef(NamedRef ref, ObjectId target, Optional<Hash> expectedHash, boolean force)
      throws IOException, ReferenceConflictException {
    RefUpdate updateBranch = repository.updateRef((ref instanceof TagName ? Constants.R_TAGS : Constants.R_HEADS) + ref.getName());
    updateBranch.setNewObjectId(target);
    fromHash(ref, expectedHash).ifPresent(updateBranch::setExpectedOldObjectId);
    Result result = force ? updateBranch.forceUpdate() : updateBranch.update();
    if (!result.equals(Result.NEW) && !result.equals(Result.FAST_FORWARD) && !result.equals(Result.FORCED)) {
      throw new ReferenceConflictException(String.format("result did not complete for update ref on %s with state %s", ref, result));
    }
  }

  private Optional<ObjectId> tryTwoWayMerge(ObjectId treeId, ObjectId newTreeId, ObjectInserter inserter, ObjectId version,
                                            List<String> unchanged) throws IOException {
    inserter.flush();

    TwoWayMerger merger = new TwoWayMerger(repository, unchanged);
    merger.setBase(version);
    boolean ok = merger.merge(treeId, newTreeId);
    return ok ? Optional.of(merger.getResultTreeId()) : Optional.empty();
  }

  private CommitBuilder fromUser(METADATA commitMeta, boolean empty) {
    CommitBuilder commitBuilder = new CommitBuilder();
    long updateTime = empty ? 0L : ZonedDateTime.now(ZoneId.of("UTC")).toInstant().toEpochMilli();
    PersonIdent person;
    try {
      UserConfig config = SystemReader.getInstance().getUserConfig().get(UserConfig.KEY);
      person = new PersonIdent(config.getAuthorName(), config.getAuthorEmail(), updateTime, 0);
    } catch (IOException | ConfigInvalidException e) {
      //todo email can't be null but we cant find it
      person = new PersonIdent(System.getProperty("user.name"), "me@example.com");
    }
    if (commitMeta != null) {
      //todo better way to store commit metadata? Make an interface? or toString?
      commitBuilder.setMessage(storeWorker.getMetadataSerializer().toBytes(commitMeta).toStringUtf8());
    } else {
      commitBuilder.setMessage("none");
    }
    commitBuilder.setAuthor(person);
    commitBuilder.setCommitter(person);
    return commitBuilder;
  }

  private static Optional<ObjectId> fromHash(NamedRef ref, Optional<Hash> s) throws ReferenceConflictException {
    try {
      return s.map(Hash::asString).map(ObjectId::fromString);
    } catch (InvalidObjectIdException e) {
      throw ReferenceConflictException.forReference(ref, s, Optional.empty(), e);
    }
  }

  private static <T> Spliterator<T> skipLastElement(Spliterator<T> src) {
    ArrayDeque<T> pending = new ArrayDeque<>(1);
    return new Spliterator<T>() {
      public boolean tryAdvance(Consumer<? super T> action) {
        boolean succeed = true;
        while (pending.size() < 2 && succeed) {
          succeed = src.tryAdvance(pending::add);
        }

        if (pending.size() > 1) {
          action.accept(pending.remove());
          return true;
        }
        return false;
      }

      public Spliterator<T> trySplit() {
        return null;
      }

      public long estimateSize() {
        return src.estimateSize() - 1;
      }

      public int characteristics() {
        return src.characteristics();
      }
    };
  }

  /**
   * URL Encode each portion of the url as separated by '/' and create a Key.
   */
  static String stringFromKey(Key key) {
    return key.getElements().stream().map(k -> {
      try {
        return URLEncoder.encode(k, StandardCharsets.UTF_8.toString());
      } catch (UnsupportedEncodingException e) {
        throw new RuntimeException(String.format("Unable to encode key %s", key), e);
      }
    }).collect(Collectors.joining(SLASH));
  }

  /**
   * URL decode each portion of the key and join into '/' separated string.
   */
  static Key keyFromUrlString(String path) {
    return Key.of(StreamSupport.stream(Arrays.spliterator(path.split(SLASH)), false)
                               .map(x -> {
                                 try {
                                   return URLDecoder.decode(x, StandardCharsets.UTF_8.toString());
                                 } catch (UnsupportedEncodingException e) {
                                   throw new RuntimeException(String.format("Unable to decode string %s", x), e);
                                 }
                               })
                               .toArray(String[]::new));
  }


  private static String refName(Ref ref) {
    final String hashName;
    if (ref instanceof BranchName) {
      hashName = ((BranchName) ref).getName();
    } else if (ref instanceof TagName) {
      hashName = ((TagName) ref).getName();
    } else if (ref instanceof Hash) {
      hashName = ((Hash) ref).asString();
    } else {
      throw new IllegalStateException(String.format("unknown ref type: %s", ref));
    }
    return hashName;
  }

  private static byte[] getTable(TreeWalk treeWalk, Repository repository)
      throws IOException {
    return getTable(treeWalk, repository, 0);
  }

  private static byte[] getTable(TreeWalk treeWalk, Repository repository, int id)
      throws IOException {
    ObjectId objectId = treeWalk.getObjectId(id);
    ObjectLoader loader = repository.open(objectId);
    return loader.getBytes();
  }

  @Override
  public Stream<Diff<WithEntityType<TABLE>>> getDiffs(Ref from, Ref to) {
    throw new UnsupportedOperationException("Not yet implemented.");
  }

}

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
package com.dremio.nessie.versioned.impl;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import javax.annotation.Nonnull;
import javax.inject.Inject;

import org.eclipse.jgit.dircache.DirCache;
import org.eclipse.jgit.dircache.DirCacheBuilder;
import org.eclipse.jgit.dircache.DirCacheEntry;
import org.eclipse.jgit.errors.AmbiguousObjectException;
import org.eclipse.jgit.errors.ConfigInvalidException;
import org.eclipse.jgit.errors.IncorrectObjectTypeException;
import org.eclipse.jgit.errors.RevisionSyntaxException;
import org.eclipse.jgit.errors.UnmergedPathException;
import org.eclipse.jgit.lib.CommitBuilder;
import org.eclipse.jgit.lib.Constants;
import org.eclipse.jgit.lib.FileMode;
import org.eclipse.jgit.lib.ObjectId;
import org.eclipse.jgit.lib.ObjectInserter;
import org.eclipse.jgit.lib.ObjectLoader;
import org.eclipse.jgit.lib.PersonIdent;
import org.eclipse.jgit.lib.RefUpdate;
import org.eclipse.jgit.lib.RefUpdate.Result;
import org.eclipse.jgit.lib.Repository;
import org.eclipse.jgit.lib.TreeFormatter;
import org.eclipse.jgit.lib.UserConfig;
import org.eclipse.jgit.merge.ThreeWayMerger;
import org.eclipse.jgit.revwalk.RevWalk;
import org.eclipse.jgit.treewalk.AbstractTreeIterator;
import org.eclipse.jgit.treewalk.NameConflictTreeWalk;
import org.eclipse.jgit.treewalk.TreeWalk;
import org.eclipse.jgit.treewalk.filter.PathFilter;
import org.eclipse.jgit.util.SystemReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.nessie.error.NessieConflictException;
import com.dremio.nessie.versioned.BranchName;
import com.dremio.nessie.versioned.Hash;
import com.dremio.nessie.versioned.Key;
import com.dremio.nessie.versioned.NamedRef;
import com.dremio.nessie.versioned.Operation;
import com.dremio.nessie.versioned.Ref;
import com.dremio.nessie.versioned.ReferenceAlreadyExistsException;
import com.dremio.nessie.versioned.ReferenceConflictException;
import com.dremio.nessie.versioned.ReferenceNotFoundException;
import com.dremio.nessie.versioned.Serializer;
import com.dremio.nessie.versioned.StoreWorker;
import com.dremio.nessie.versioned.TagName;
import com.dremio.nessie.versioned.Unchanged;
import com.dremio.nessie.versioned.VersionStore;
import com.dremio.nessie.versioned.WithHash;
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
        repository.resolve(refOfUnknownType + "^{tree}");
        return WithHash.of(Hash.of(refOfUnknownType), Hash.of(refOfUnknownType));
      } catch (AmbiguousObjectException | IncorrectObjectTypeException | RevisionSyntaxException e){
        throw new ReferenceNotFoundException(String.format("Unable to find the requested reference %s.", refOfUnknownType));
      }
    } catch (IOException e) {
      throw new RuntimeException("Error talking to git repo", e);
    }
  }

  @Override
  public void commit(BranchName branch, Optional<Hash> expectedHash, METADATA metadata,
                     List<Operation<TABLE>> operations) throws ReferenceNotFoundException, ReferenceConflictException {
    toHash(branch);
    try {
      ObjectId commits = TreeBuilder.commitObjects(operations, repository, storeWorker.getValueSerializer(), emptyObject);
      ObjectId treeId = repository.resolve(expectedHash.map(Hash::asString).orElse(branch.getName()) + "^{tree}");
      ObjectId newTree = TreeBuilder.merge(treeId, commits, repository);
      try {
        commitTree(branch, newTree, expectedHash, metadata);
      } catch (NessieConflictException e) {
        if (operations.stream().anyMatch(x -> x instanceof Unchanged)) {
          throw e;
        }
        ObjectId currentTreeId = repository.resolve(branch.getName() + "^{tree}");
        ObjectId currentCommitId = repository.resolve(branch.getName() + "^{commit}");
        Optional<ObjectId> mergedTree = tryTwoWayMerge(currentTreeId,
                                                       newTree,
                                                       repository.newObjectInserter(),
                                                       expectedHash.map(Hash::asString).map(ObjectId::fromString).orElseThrow(() -> e));
        commitTree(branch, mergedTree.orElseThrow(() -> e), Optional.of(currentCommitId).map(ObjectId::name).map(Hash::of), metadata);
      }
    } catch (NessieConflictException e) {
      throw ReferenceConflictException.forReference(branch, expectedHash, Optional.empty(), e);
    } catch (IOException e) {
      throw new RuntimeException("Unknown error", e);
    }
  }

  @Override
  public void transplant(BranchName targetBranch, Optional<Hash> expectedHash,
                         List<Hash> sequenceToTransplant) {
    throw new IllegalStateException("Not yet implemented.");
  }

  @Override
  public void merge(Hash fromHash, BranchName toBranch, Optional<Hash> expectedHash) {
    throw new IllegalStateException("Not yet implemented.");
  }

  @Override
  public void assign(NamedRef ref, Optional<Hash> expectedHash, Hash targetHash)
      throws ReferenceNotFoundException, ReferenceConflictException {
    try {
      Hash existingHash = toHash(ref);
      if (expectedHash.isPresent() && !existingHash.equals(expectedHash.get())) {
        throw new ReferenceConflictException(String.format("expected hash %s does not match current hash %s", expectedHash, existingHash));
      }
    } catch (ReferenceNotFoundException e) {
      //ref doesn't exist so create it
      if (expectedHash.isPresent()) {
        throw new ReferenceNotFoundException(String.format("Ref %s does not exist and expected hash does", ref));
      }
      try {
        create(ref, Optional.of(targetHash));
      } catch (ReferenceAlreadyExistsException pass) {
        //can't happen
      }
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
      throw new ReferenceNotFoundException("You must provide a target hash to create a tag.");
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
        commitTree((BranchName)ref, newTreeId, Optional.empty(), null);
      } else {
        ObjectId target = repository.resolve(targetHash.get().asString());
        RefUpdate createBranch = repository.updateRef((ref instanceof TagName ? Constants.R_TAGS : Constants.R_HEADS) + ref.getName());
        createBranch.setNewObjectId(target);
        Result result = createBranch.update();
        if (result.equals(Result.REJECTED_MISSING_OBJECT)) {
          throw ReferenceNotFoundException.forReference(targetHash.get());
        } else if (!result.equals(Result.NEW)) {
          throw new NessieConflictException(String.format("result did not complete for create branch on %s with state %s", ref, result));
        }
      }
    } catch (IOException e) {
      throw new RuntimeException(String.format("Unknown error while creating %s", ref), e);
    }
  }

  @Override
  public void delete(NamedRef ref, Optional<Hash> hash) throws ReferenceNotFoundException, ReferenceConflictException {
    toHash(ref);
    try {
      RefUpdate update = repository.updateRef((ref instanceof TagName ? Constants.R_TAGS : Constants.R_HEADS) + ref.getName());
      Optional<ObjectId> objectId = hash.map(Hash::asString).map(ObjectId::fromString);
      if (objectId.isPresent() && !ObjectId.isEqual(update.getRef().getObjectId(), objectId.get())) {
        throw new NessieConflictException(null, "can't delete branch, not HEAD");
      }
      update.setForceUpdate(true);
      objectId.ifPresent(update::setExpectedOldObjectId);
      Result deleteResult = update.delete();
      if (deleteResult.equals(Result.REJECTED_MISSING_OBJECT)) {
        throw ReferenceNotFoundException.forReference(hash.get());
      } else if (!deleteResult.equals(Result.FORCED)) {
        throw new NessieConflictException(ImmutableList.of(ref.getName()), "delete failed " + deleteResult);
      }
    } catch (NessieConflictException e) {
      throw ReferenceConflictException.forReference(ref, hash, Optional.empty(), e);
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
      return StreamSupport.stream(walk.spliterator(), false)
                          .map(r -> {
                            Hash hash = Hash.of(r.name());
                            METADATA metadata = serializer.fromBytes(ByteString.copyFrom(r.getFullMessage(), StandardCharsets.UTF_8));
                            return WithHash.of(hash, metadata);
                          });
    } catch (IOException e) {
      throw new RuntimeException("Unknown error", e);
    }
  }

  @Override
  public Stream<Key> getKeys(Ref ref) {
    try {
      List<Key> tables = new ArrayList<>();
      try (TreeWalk treeWalk = new TreeWalk(repository)) {
        ObjectId treeId = repository.resolve(refName(ref) + "^{tree}");
        treeWalk.addTree(treeId);
        treeWalk.setRecursive(true);
        while (treeWalk.next()) {
          tables.add(keyFromUrlString(treeWalk.getPathString()));
        }
      }
      return tables.stream();
    } catch (IOException e) {
      throw new RuntimeException("Unknown error", e);
    }
  }

  @Override
  public TABLE getValue(Ref ref, Key key) throws ReferenceNotFoundException {
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
          return storeWorker.getValueSerializer().fromBytes(ByteString.copyFrom(bytes));
        }
      }
      return null;
    } catch (IOException e) {
      throw new ReferenceNotFoundException(String.format("reference for ref %s and key %s not found", ref, key), e);
    }
  }

  @Override
  public List<Optional<TABLE>> getValue(Ref ref, List<Key> key) {
    throw new IllegalStateException("Not yet implemented.");
  }

  @Override
  public Collector collectGarbage() {
    throw new IllegalStateException("Not yet implemented.");
  }

  private void commitTree(BranchName branch, ObjectId newTree, Optional<Hash> expectedHash, METADATA metadata) throws IOException {
    ObjectInserter inserter = repository.newObjectInserter();
    CommitBuilder commitBuilder = fromUser(metadata);
    commitBuilder.setTreeId(newTree);
    ObjectId parentId = expectedHash.map(Hash::asString)
                                    .map(ObjectId::fromString)
                                    .orElse(repository.resolve(Constants.R_HEADS + branch.getName()));
    if (parentId != null) {
      commitBuilder.setParentId(parentId);
    }
    ObjectId newCommitId = inserter.insert(commitBuilder);
    inserter.flush();
    updateRef(branch, newCommitId, expectedHash, false);
  }

  private void updateRef(NamedRef ref, Hash targetHash, Optional<Hash> expectedHash, boolean force) throws IOException {
    ObjectId target = repository.resolve(targetHash.asString());
    updateRef(ref, target, expectedHash, force);
  }

  private void updateRef(NamedRef ref, ObjectId target, Optional<Hash> expectedHash, boolean force) throws IOException {
    RefUpdate updateBranch = repository.updateRef((ref instanceof TagName ? Constants.R_TAGS : Constants.R_HEADS) + ref.getName());
    updateBranch.setNewObjectId(target);
    expectedHash.map(Hash::asString).map(ObjectId::fromString).ifPresent(updateBranch::setExpectedOldObjectId);
    Result result = force ? updateBranch.forceUpdate() : updateBranch.update();
    if (!result.equals(Result.NEW) && !result.equals(Result.FAST_FORWARD) && !result.equals(Result.FORCED)) {
      throw new NessieConflictException(String.format("result did not complete for create branch on %s with state %s", ref, result));
    }
  }

  private Optional<ObjectId> tryTwoWayMerge(ObjectId treeId, ObjectId newTreeId, ObjectInserter inserter, ObjectId version)
      throws IOException {
    inserter.flush();

    Merger merger = new Merger(repository);
    merger.setBase(version);
    boolean ok = merger.merge(treeId, newTreeId);
    return ok ? Optional.of(merger.getResultTreeId()) : Optional.empty();
  }

  private CommitBuilder fromUser(METADATA commitMeta) {
    CommitBuilder commitBuilder = new CommitBuilder();
    long updateTime = ZonedDateTime.now(ZoneId.of("UTC")).toInstant().toEpochMilli();
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

  /**
   * Simple merge of two potentially conflicting branches.
   * <p>
   *   If no file level conflicts exist the merge will succeed. Any file level merges will result in failure.
   * </p>
   */
  private static class Merger extends ThreeWayMerger {

    private static final int T_BASE = 0;

    private static final int T_OURS = 1;

    private static final int T_THEIRS = 2;

    private final NameConflictTreeWalk tw;

    private final DirCache cache;

    private DirCacheBuilder builder;

    private ObjectId resultTree;

    Merger(Repository local) {
      super(local);
      tw = new NameConflictTreeWalk(local, reader);
      cache = DirCache.newInCore();
    }

    @Override
    protected boolean mergeImpl() throws IOException {
      tw.addTree(mergeBase());
      tw.addTree(sourceTrees[0]);
      tw.addTree(sourceTrees[1]);

      boolean hasConflict = false;
      builder = cache.builder();
      while (tw.next()) {
        final int modeO = tw.getRawMode(T_OURS);
        final int modeT = tw.getRawMode(T_THEIRS);
        if (modeO == modeT && tw.idEqual(T_OURS, T_THEIRS)) {
          add(T_OURS, DirCacheEntry.STAGE_0);
          continue;
        }

        final int modeB = tw.getRawMode(T_BASE);
        if (modeB == modeO && tw.idEqual(T_BASE, T_OURS)) {
          add(T_THEIRS, DirCacheEntry.STAGE_0);
        } else if (modeB == modeT && tw.idEqual(T_BASE, T_THEIRS)) {
          if (!ObjectId.isEqual(tw.getObjectId(T_BASE), ObjectId.zeroId())) {
            add(T_OURS, DirCacheEntry.STAGE_0); // only add if object has changed. If it was null before and now ignore it.
          }
        } else {
          if (nonTree(modeB)) {
            add(T_BASE, DirCacheEntry.STAGE_1);
            hasConflict = true;
          }
          if (nonTree(modeO)) {
            add(T_OURS, DirCacheEntry.STAGE_2);
            hasConflict = true;
          }
          if (nonTree(modeT)) {
            add(T_THEIRS, DirCacheEntry.STAGE_3);
            hasConflict = true;
          }
          if (tw.isSubtree()) {
            tw.enterSubtree();
          }
        }
      }
      builder.finish();
      builder = null;

      if (hasConflict) {
        return false;
      }
      try {
        ObjectInserter odi = getObjectInserter();
        resultTree = cache.writeTree(odi);
        odi.flush();
        return true;
      } catch (UnmergedPathException upe) {
        resultTree = null;
        return false;
      }
    }

    private static boolean nonTree(int mode) {
      return mode != 0 && !FileMode.TREE.equals(mode);
    }

    private void add(int tree, int stage) throws IOException {
      final AbstractTreeIterator i = getTree(tree);
      if (i != null) {
        if (FileMode.TREE.equals(tw.getRawMode(tree))) {
          builder.addTree(tw.getRawPath(), stage, reader, tw.getObjectId(tree));
        } else {
          final DirCacheEntry e;

          e = new DirCacheEntry(tw.getRawPath(), stage);
          e.setObjectIdFromRaw(i.idBuffer(), i.idOffset());
          e.setFileMode(tw.getFileMode(tree));
          builder.add(e);
        }
      }
    }

    private AbstractTreeIterator getTree(int tree) {
      return tw.getTree(tree, AbstractTreeIterator.class);
    }

    @Override
    public ObjectId getResultTreeId() {
      return resultTree;
    }
  }
}

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

package com.dremio.nessie.jgit;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.LogCommand;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.eclipse.jgit.dircache.DirCache;
import org.eclipse.jgit.dircache.DirCacheBuilder;
import org.eclipse.jgit.dircache.DirCacheEntry;
import org.eclipse.jgit.errors.CheckoutConflictException;
import org.eclipse.jgit.errors.UnmergedPathException;
import org.eclipse.jgit.internal.storage.dfs.DfsRepositoryDescription;
import org.eclipse.jgit.lib.CommitBuilder;
import org.eclipse.jgit.lib.Constants;
import org.eclipse.jgit.lib.FileMode;
import org.eclipse.jgit.lib.ObjectId;
import org.eclipse.jgit.lib.ObjectInserter;
import org.eclipse.jgit.lib.ObjectLoader;
import org.eclipse.jgit.lib.Ref;
import org.eclipse.jgit.lib.RefUpdate;
import org.eclipse.jgit.lib.RefUpdate.Result;
import org.eclipse.jgit.lib.Repository;
import org.eclipse.jgit.lib.TreeFormatter;
import org.eclipse.jgit.merge.ThreeWayMerger;
import org.eclipse.jgit.revwalk.RevCommit;
import org.eclipse.jgit.revwalk.RevWalk;
import org.eclipse.jgit.treewalk.AbstractTreeIterator;
import org.eclipse.jgit.treewalk.NameConflictTreeWalk;
import org.eclipse.jgit.treewalk.TreeWalk;
import org.eclipse.jgit.treewalk.filter.PathFilter;
import org.eclipse.jgit.util.sha1.SHA1;

import com.dremio.nessie.backend.Backend;
import com.dremio.nessie.backend.BranchController;
import com.dremio.nessie.backend.ImmutableLogMessage;
import com.dremio.nessie.backend.LogMessage;
import com.dremio.nessie.backend.TableConverter;
import com.dremio.nessie.error.NessieConflictException;
import com.dremio.nessie.model.Branch;
import com.dremio.nessie.model.ImmutableBranch;
import com.dremio.nessie.model.Table;
import com.dremio.nessie.versioned.Serializer;
import com.dremio.nessie.versioned.StoreWorker;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;

public abstract class JgitBranchController<TABLE, METADATA> implements BranchController<TABLE, METADATA> {

  private static final Joiner SLASH = Joiner.on("/");

  private final Repository repository;
  private final StoreWorker<TABLE, METADATA> storeWorker;

  public JgitBranchController(StoreWorker<TABLE, METADATA> storeWorker, Repository repository) {
    this.storeWorker = storeWorker;
    this.repository = repository;
  }

  /**
   * Construct a JgitBranchController. This uses jgit to fulfill the BranchController contract.
   * @param backend db backend to work off of
   */
  public JgitBranchController(Function<Repository, StoreWorker<TABLE, METADATA>> storeWorker, Backend backend) {
    DfsRepositoryDescription repoDesc = new DfsRepositoryDescription();
    Repository repository;
    try {
      repository = new NessieRepository.Builder().setRepositoryDescription(repoDesc)
                                                 .setBackend(backend.gitBackend())
                                                 .setRefBackend(backend.gitRefBackend())
                                                 .build();
    } catch (IOException e) {
      //pass can't happen
      repository = null;
    }
    this.repository = repository;
    this.storeWorker = storeWorker.apply(repository);
  }

  @Override
  public Branch create(String branch,
                       String baseBranch,
                       METADATA commitMeta,
                       TableConverter<TABLE> tableConverter) throws IOException {
    String headVersion;
    if (branch.equals("master") && (baseBranch == null || baseBranch.equals(branch))) {
      TreeFormatter formatter = new TreeFormatter();
      headVersion = commitTree(formatter, branch, commitMeta, null, tableConverter);
    } else {
      String base = baseBranch == null ? Constants.MASTER : baseBranch;
      Ref master = repository.findRef(Constants.R_HEADS + base);
      ObjectId masterTip = master.getObjectId();
      RefUpdate createBranch = repository.updateRef(Constants.R_HEADS + branch);
      createBranch.setNewObjectId(masterTip);
      Result result = createBranch.update();
      if (!result.equals(Result.NEW)) {
        throw new NessieConflictException(null,
          "result did not complete for create branch on " + branch + " with state " + result);
      }
      headVersion = masterTip.name();
    }
    return ImmutableBranch.builder().name(branch).id(headVersion).build();
  }

  @Override
  public List<Branch> getBranches() throws IOException {
    return repository.getRefDatabase()
                     .getRefsByPrefix(Constants.R_HEADS)
                     .stream()
                     .map(JgitBranchController::fromRef)
                     .collect(Collectors.toList());

  }

  @Override
  public Branch getBranch(String branch) throws IOException {
    repository.getRefDatabase().refresh();
    Ref ref = repository.findRef(Constants.R_HEADS + branch);
    if (ref == null) {
      return null;
    }
    return fromRef(ref);
  }

  @Override
  public TABLE getTable(String branch, String table, boolean metadata) throws IOException {
    try (TreeWalk treeWalk = new TreeWalk(repository)) {
      ObjectId treeId = repository.resolve(branch + "^{tree}");
      treeWalk.addTree(treeId);
      treeWalk.setRecursive(true);
      treeWalk.setFilter(PathFilter.create(directoryFor(table)));
      while (treeWalk.next()) {
        byte[] bytes = getTable(treeWalk, repository);
        return storeWorker.getValueSerializer().fromBytes(ByteString.copyFrom(bytes));
      }
    }
    return null;
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

  private static ObjectId idFor(String filename) {
    SHA1 md = SHA1.newInstance();
    md.update(filename.getBytes(StandardCharsets.US_ASCII));
    return md.toObjectId();
  }

  private static String directoryFor(String tableName) {
    Entry<String, String> pair = fileFolderFor(tableName);
    return SLASH.join(pair.getKey(), pair.getValue());
  }

  private static Entry<String, String> fileFolderFor(String tableName) {
    ObjectId fileId = idFor(tableName);
    String object = fileId.name();
    return new SimpleImmutableEntry<>(object.substring(0, 2), object.substring(2));
  }

  private Map<String, Map<String, ObjectId>> commitObjects(TABLE[] tables, TableConverter<TABLE> tableConverter) throws IOException {
    ObjectInserter inserter = repository.newObjectInserter();
    Map<String, Map<String, ObjectId>> commits = new HashMap<>();
    for (TABLE branchTable : tables) {
      ObjectId blobId = null;
      if (!tableConverter.isDeleted(branchTable)) {
        byte[] data = storeWorker.getValueSerializer().toBytes(branchTable).toByteArray();
        blobId = inserter.insert(Constants.OBJ_BLOB, data);
      }
      Entry<String, String> pair = fileFolderFor(tableConverter.getId(branchTable));
      String destinationFolder = pair.getKey();
      String destinationFilename = pair.getValue();
      if (!commits.containsKey(destinationFolder)) {
        commits.put(destinationFolder, new HashMap<>());
      }
      commits.get(destinationFolder).put(destinationFilename, blobId);
    }
    inserter.flush();
    return commits;
  }

  private ObjectId commitSubtree(TreeWalk treeWalk, String folder, Map<String, ObjectId> filenames)
      throws IOException {
    final ObjectInserter inserter = repository.newObjectInserter();
    treeWalk.enterSubtree();
    int depth = treeWalk.getDepth();
    TreeFormatter subTreeFormatter = new TreeFormatter();
    boolean inserted = false;
    while (treeWalk.next() && treeWalk.getDepth() == depth) {
      String path = treeWalk.getPathString();
      String filename = path.replaceFirst(folder + "/", "");
      if (filenames.containsKey(filename)) {
        ObjectId blobId = filenames.remove(filename);
        if (blobId != null) {
          subTreeFormatter.append(filename, FileMode.REGULAR_FILE, blobId);
          inserted = true;
        }
      } else {
        subTreeFormatter.append(filename, FileMode.REGULAR_FILE, treeWalk.getObjectId(0));
        inserted = true;
      }
    }
    if (!filenames.isEmpty()) {
      for (Entry<String, ObjectId> entry : filenames.entrySet()) {
        if (entry.getValue() != null) {
          inserted = true;
          subTreeFormatter.append(entry.getKey(), FileMode.REGULAR_FILE, entry.getValue());
        }
      }
      filenames.clear();
    }
    if (inserted) {
      ObjectId subTreeId = inserter.insert(subTreeFormatter);
      inserter.flush();
      return subTreeId;
    }
    return null;
  }

  private ObjectId newSubTree(Map<String, ObjectId> filenames) throws IOException {
    ObjectInserter inserter = repository.newObjectInserter();
    TreeFormatter subTreeFormatter = new TreeFormatter();
    boolean added = false;
    for (Entry<String, ObjectId> entry : filenames.entrySet()) {
      if (entry.getValue() == null) {
        continue;
      }
      added = true;
      subTreeFormatter.append(entry.getKey(), FileMode.REGULAR_FILE, entry.getValue());
    }
    filenames.clear();
    if (added) {
      ObjectId subTreeId = inserter.insert(subTreeFormatter);
      inserter.flush();
      return subTreeId;
    }
    return null;
  }

  @Override
  public String commit(String branch,
                       METADATA commitMeta,
                       String version,
                       TableConverter<TABLE> tableConverter,
                       TABLE... tables) throws IOException {
    ObjectId treeId = repository.resolve(version + "^{tree}");
    TreeWalk treeWalk = new TreeWalk(repository);
    treeWalk.addTree(treeId);
    treeWalk.setRecursive(false);
    TreeFormatter treeFormatter = new TreeFormatter();
    Map<String, Map<String, ObjectId>> commits = commitObjects(tables, tableConverter);

    while (treeWalk.next()) {
      if (treeWalk.getObjectId(0).equals(ObjectId.zeroId())) {
        continue;
      }
      boolean goAgain = true;
      while (goAgain) {
        String folder = treeWalk.getPathString();
        if (commits.containsKey(folder)) {
          Map<String, ObjectId> filenames = commits.remove(folder);
          ObjectId subTreeId = commitSubtree(treeWalk, folder, filenames);
          goAgain = !treeWalk.getObjectId(0).equals(ObjectId.zeroId());
          if (subTreeId != null) {
            treeFormatter.append(folder, FileMode.TREE, subTreeId);
          }
        } else {
          goAgain = false;
          treeFormatter.append(folder, FileMode.TREE, treeWalk.getObjectId(0));
        }
      }
    }
    if (!commits.isEmpty()) {
      for (String destinationFolder : commits.keySet()) {
        Map<String, ObjectId> filenames = commits.get(destinationFolder);
        ObjectId subTreeId = newSubTree(filenames);
        if (subTreeId != null) {
          treeFormatter.append(destinationFolder, FileMode.TREE, subTreeId);
        }
      }
      commits.clear();
    }
    return commitTree(treeFormatter,
                      branch,
                      commitMeta,
                      version,
                      tableConverter);
  }

  @Override
  public void deleteBranch(String branch,
                           String version,
                           METADATA commitMeta) throws IOException {
    RefUpdate update = repository.updateRef(Constants.R_HEADS + branch);
    if (version != null && !ObjectId.isEqual(update.getRef().getObjectId(), ObjectId.fromString(version))) {
      throw new NessieConflictException(null, "can't delete branch, not HEAD");
    }
    update.setRefLogMessage(commitMeta.toString(), false);
    update.setForceUpdate(true);
    update.setExpectedOldObjectId(ObjectId.fromString(version));
    Result deleteResult = update.delete();  // todo concurrency & check
    if (!deleteResult.equals(Result.FORCED)) {
      throw new NessieConflictException(ImmutableList.of(branch), "delete failed " + deleteResult);
    }
  }

  @Override
  public List<String> getTables(String branch, String namespace, TableConverter<TABLE> tableConverter) throws IOException {
    List<String> tables = new ArrayList<>();
    try (TreeWalk treeWalk = new TreeWalk(repository)) {
      ObjectId treeId = repository.resolve(branch + "^{tree}");
      treeWalk.addTree(treeId);
      treeWalk.setRecursive(true);
      while (treeWalk.next()) {
        byte[] bytes = getTable(treeWalk, repository);
        TABLE branchTable = storeWorker.getValueSerializer().fromBytes(ByteString.copyFrom(bytes));
        if (namespace != null && !namespace.equals(tableConverter.getNamespace(branchTable))) {
          continue;
        }
        tables.add(tableConverter.getId(branchTable));
      }
    }
    return tables;
  }

  @Override
  public String promote(String branch,
                        String mergeBranch,
                        String version,
                        METADATA commitMeta,
                        boolean force,
                        boolean cherryPick,
                        String namespace,
                        TableConverter<TABLE> tableConverter) throws IOException {
    ObjectId newCommitId = repository.resolve(mergeBranch + "^{commit}");
    RevCommit newCommit = RevCommit.parse(repository.getObjectDatabase()
                                                    .open(newCommitId)
                                                    .getBytes());
    Ref head = repository.findRef(branch);
    checkVersion(version, branch);
    return rebase(head, newCommit, version, commitMeta, force, cherryPick, namespace, tableConverter);
  }

  private ObjectId checkVersion(String version, String branch) {
    if (version == null) {
      return null;
    }
    ObjectId commitId;
    try {
      commitId = repository.resolve(branch + "^{commit}");
    } catch (IOException e) {
      commitId = null;
    }
    if (!ObjectId.isEqual(commitId, ObjectId.fromString(version))) {
      throw new NessieConflictException(null, "version commit doesn't equal current HEAD");
    }
    return commitId;
  }

  @Override
  public Stream<LogMessage> log(String branch) throws IOException {
    ObjectId objectId = repository.resolve(branch);
    if (objectId == null) {
      throw new IllegalStateException();
    }
    RevWalk walk = new RevWalk(repository);
    walk.markStart(repository.parseCommit(objectId));
    return StreamSupport.stream(walk.spliterator(), false).map(JgitBranchController::toLogMessage);
  }

  private static LogMessage toLogMessage(RevCommit commit) {
    return ImmutableLogMessage.builder().commitId(commit.name()).message(commit.getFullMessage()).build();
  }

  private String commitTree(ObjectId newTreeId,
                            String branch,
                            METADATA commitMeta,
                            String version,
                            ObjectInserter inserter,
                            TableConverter<TABLE> tableConverter) throws IOException {

    ObjectId commitId;
    try {
      commitId = checkVersion(version, branch);
    } catch (NessieConflictException e) {
      Entry<ObjectId, List<String>> pair = tryTwoWayMerge(branch,
                                                          newTreeId,
                                                          commitMeta,
                                                          inserter,
                                                          version,
                                                          tableConverter);
      commitId = pair.getKey();
      if (commitId == null) {
        throw new NessieConflictException(pair.getValue(), "conflicted files", e);
      }
    }
    inserter.flush();
    CommitBuilder commitBuilder = fromUser(commitMeta);
    commitBuilder.setTreeId(newTreeId);
    if (commitId != null) {
      commitBuilder.addParentId(commitId);
    }
    ObjectId newCommitId = inserter.insert(commitBuilder);
    inserter.flush();
    return updateRef(branch, newCommitId, commitId);
  }

  private String commitTree(TreeFormatter treeFormatter,
                            String branch,
                            METADATA commitMeta,
                            String version,
                            TableConverter<TABLE> tableConverter) throws IOException {
    ObjectInserter inserter = repository.newObjectInserter();
    ObjectId newTreeId = inserter.insert(treeFormatter);
    return commitTree(newTreeId, branch, commitMeta, version, inserter, tableConverter);
  }

  private Entry<ObjectId, List<String>> tryTwoWayMerge(String branch,
                                                     ObjectId newTreeId,
                                                       METADATA commitMeta,
                                                     ObjectInserter inserter,
                                                     String version,
                                                       TableConverter<TABLE> tableConverter) throws IOException {
    inserter.flush();
    ObjectId commitId = repository.resolve(branch + "^{commit}");
    ObjectId treeId = repository.resolve(branch + "^{tree}");
    Merger<TABLE> merger = new Merger<>(repository, storeWorker.getValueSerializer(), tableConverter, null);
    merger.setBase(ObjectId.fromString(version));
    boolean ok = merger.merge(treeId, newTreeId);
    if (!ok) {
      return new SimpleImmutableEntry<>(null, merger.conflictFiles());
    }
    ObjectId mergedTreeId = merger.getResultTreeId();
    String commit = commitTree(mergedTreeId,
                               branch,
                               commitMeta,
                               commitId.name(),
                               inserter,
                               tableConverter);
    return new SimpleImmutableEntry<>(ObjectId.fromString(commit), null);
  }

  private String updateRef(String branch, ObjectId newCommitId, ObjectId version)
      throws IOException {
    RefUpdate ref = repository.updateRef(
        branch.contains(Constants.R_HEADS) ? branch : (Constants.R_HEADS + branch));
    ref.setNewObjectId(newCommitId);
    if (version != null) {
      ref.setExpectedOldObjectId(version);
    }
    Result result = ref.update();
    if (!result.equals(Result.NEW) && !result.equals(Result.FAST_FORWARD)) {
      throw new NessieConflictException(null,
        "Unable to complete commit and update Ref " + ref.getRef() + ", result was " + result);
    }
    return newCommitId.name();
  }

  protected abstract CommitBuilder fromUser(METADATA commitMeta);

  private static Branch fromRef(Ref ref) {
    if (ref == null) {
      return null;
    }
    return ImmutableBranch.builder()
                          .id(ref.getObjectId().name())
                          .name(ref.getName().replaceFirst(Constants.R_HEADS, ""))
                          .isDeleted(false)
                          .updateTime(Long.MIN_VALUE)
                          .build();
  }

  private String rebase(Ref head,
                        RevCommit upstreamCommit,
                        String version,
                        METADATA commitMeta,
                        boolean force,
                        boolean cherryPick,
                        String namespace,
                        TableConverter<TABLE> tableConverter) throws IOException {
    RevWalk walk = new RevWalk(repository);
    ObjectId headId = head.getObjectId();
    String headName = head.getName();
    RevCommit headCommit = walk.lookupCommit(headId);
    RevCommit upstream = walk.lookupCommit(upstreamCommit.getId());

    if (walk.isMergedInto(upstream, headCommit)) {
      return headCommit.getId().name();
    } else if (walk.isMergedInto(headCommit, upstream) || force) {
      updateHead(headName, upstreamCommit, upstream, version);
      return upstream.getId().name();
    }

    if (!cherryPick) {
      throw new NessieConflictException(null, "unable to perform merge without cherry-pick");
    }

    List<RevCommit> pickList = calculatePickList(upstreamCommit, headCommit);
    for (RevCommit step : pickList) {
      //currently this will attempt to merge entire database.
      version = cherryPickCommitFlattening(step, head, version, commitMeta, namespace, tableConverter);
    }
    return version;
  }

  private String cherryPickCommitFlattening(RevCommit commitToPick,
                                            Ref head,
                                            String version,
                                            METADATA commitMeta,
                                            String namespace,
                                            TableConverter<TABLE> tableConverter) throws IOException {
    RevCommit newHead = tryFastForward(commitToPick, head);
    boolean lastStepWasForward = newHead != null;
    if (lastStepWasForward) {
      return updateRef(head.getName(), newHead.getId(), ObjectId.fromString(version));
    }
    RevWalk revWalk = new RevWalk(repository);
    Merger<TABLE> merger = new Merger<>(repository, storeWorker.getValueSerializer(), tableConverter, namespace);
    newHead = revWalk.parseCommit(head.getObjectId());
    boolean ok = merger.merge(commitToPick, newHead);
    if (ok) {
      ObjectId newTree = merger.getResultTreeId();
      return commitTree(newTree,
                        head.getName(),
                        commitMeta,
                        version,
                        repository.newObjectInserter(),
                        tableConverter);
    } else {
      throw new NessieConflictException(merger.conflictFiles(), "rebase failed");
    }
  }

  private RevCommit tryFastForward(RevCommit newCommit, Ref head) throws IOException {
    RevWalk walk = new RevWalk(repository);
    ObjectId headId = head.getObjectId();
    RevCommit headCommit = walk.lookupCommit(headId);
    if (walk.isMergedInto(newCommit, headCommit)) {
      return newCommit;
    }
    String headName = head.getObjectId().name();
    boolean tryRebase = false;
    for (RevCommit parentCommit : newCommit.getParents()) {
      if (parentCommit.equals(headCommit)) {
        tryRebase = true;
        break;
      }
    }
    if (!tryRebase) {
      return null;
    }

    try {
      if (headName.startsWith(Constants.R_HEADS)) {
        RefUpdate rup = repository.updateRef(headName);
        rup.setExpectedOldObjectId(headCommit);
        rup.setNewObjectId(newCommit);
        rup.setRefLogMessage("Fast-forward from " + headCommit.name()
                             + " to " + newCommit.name(), false);
        Result res = rup.update(walk);
        switch (res) {
          case FAST_FORWARD:
          case NO_CHANGE:
          case FORCED:
            break;
          default:
            throw new NessieConflictException(null, "Could not fast-forward");
        }
      }
      return newCommit;
    } catch (CheckoutConflictException e) {
      throw new NessieConflictException(null, e.getMessage(), e);
    }
  }

  private void updateHead(String headName, RevCommit newHead, RevCommit onto, String version)
      throws IOException {
    if (headName.startsWith(Constants.R_REFS)) {
      RefUpdate rup = repository.updateRef(headName);
      rup.setNewObjectId(newHead);
      rup.setExpectedOldObjectId(ObjectId.fromString(version));
      rup.setRefLogMessage("rebase finished: " + headName + " onto " //$NON-NLS-1$ //$NON-NLS-2$
                           + onto.getName(), false);
      Result res = rup.forceUpdate();
      switch (res) {
        case FAST_FORWARD:
        case FORCED:
        case NO_CHANGE:
          break;
        default:
          throw new IOException("failed update");
      }
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


  private static class Merger<TABLE> extends ThreeWayMerger {

    private static final int T_BASE = 0;

    private static final int T_OURS = 1;

    private static final int T_THEIRS = 2;

    private final NameConflictTreeWalk tw;
    private final Serializer<TABLE> serializer;
    private final TableConverter<TABLE> tableConverter;
    private final String namespace;

    private final DirCache cache;

    private DirCacheBuilder builder;

    private ObjectId resultTree;

    private final List<String> conflictFiles = new ArrayList<>();

    Merger(Repository local, Serializer<TABLE> serializer, TableConverter<TABLE> tableConverter, String namespace) {
      super(local);
      tw = new NameConflictTreeWalk(local, reader);
      this.serializer = serializer;
      this.tableConverter = tableConverter;
      this.namespace = namespace;
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
        if (!inNamespace()) {
          continue;
        }
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
          add(T_OURS, DirCacheEntry.STAGE_0);
        } else {
          if (nonTree(modeB)) {
            add(T_BASE, DirCacheEntry.STAGE_1);
            addConflict(T_BASE);
            hasConflict = true;
          }
          if (nonTree(modeO)) {
            add(T_OURS, DirCacheEntry.STAGE_2);
            addConflict(T_OURS);
            hasConflict = true;
          }
          if (nonTree(modeT)) {
            add(T_THEIRS, DirCacheEntry.STAGE_3);
            addConflict(T_THEIRS);
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

    private void addConflict(int id) {
      try {
        byte[] bytes = getTable(tw, db, id);
        TABLE entry = serializer.fromBytes(ByteString.copyFrom(bytes));
        conflictFiles.add(tableConverter.getId(entry));
      } catch (IOException e) {
        //pass
      }
    }

    public List<String> conflictFiles() {
      return conflictFiles;
    }

    private boolean inNamespace() throws IOException {
      if (namespace == null) {
        return true;
      }
      ObjectId objectId = tw.getObjectId(0);
      ObjectLoader loader = db.open(objectId);
      Entry<Table, String> branchTablePair = ProtoUtil.tableFromBytes(loader.getBytes());
      Table branchTable = branchTablePair.getKey();
      return namespace.equals(branchTable.getNamespace());
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

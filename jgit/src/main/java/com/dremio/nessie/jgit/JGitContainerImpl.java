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

import com.dremio.nessie.backend.Backend;
import com.dremio.nessie.model.Branch;
import com.dremio.nessie.model.BranchTable;
import com.dremio.nessie.model.HeadVersionPair;
import com.dremio.nessie.model.ImmutableBranch;
import com.dremio.nessie.model.VersionedStringWrapper;
import com.google.common.base.Joiner;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.Principal;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.stream.Collectors;
import org.eclipse.jgit.internal.storage.dfs.DfsRepositoryDescription;
import org.eclipse.jgit.lib.CommitBuilder;
import org.eclipse.jgit.lib.Constants;
import org.eclipse.jgit.lib.FileMode;
import org.eclipse.jgit.lib.ObjectId;
import org.eclipse.jgit.lib.ObjectInserter;
import org.eclipse.jgit.lib.ObjectLoader;
import org.eclipse.jgit.lib.PersonIdent;
import org.eclipse.jgit.lib.Ref;
import org.eclipse.jgit.lib.RefUpdate;
import org.eclipse.jgit.lib.RefUpdate.Result;
import org.eclipse.jgit.lib.Repository;
import org.eclipse.jgit.lib.TreeFormatter;
import org.eclipse.jgit.treewalk.TreeWalk;
import org.eclipse.jgit.treewalk.filter.PathFilter;
import org.eclipse.jgit.util.sha1.SHA1;

public class JGitContainerImpl implements JGitContainer {

  private static final Joiner DOT = Joiner.on('.');
  private static final Joiner SLASH = Joiner.on("/");

  private final Repository repository;

  public JGitContainerImpl(Backend backend) {
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
  }

  @Override
  public VersionedStringWrapper<Branch> create(String branch,
                                               String baseBranch,
                                               Principal principal) throws IOException {
    HeadVersionPair headVersion = null;
    if (branch.equals("master") && baseBranch == null) {
      TreeFormatter formatter = new TreeFormatter();
      long updateTime = ZonedDateTime.now(ZoneId.of("UTC")).toInstant().toEpochMilli();
      headVersion = commitTree(formatter, branch, updateTime, principal, null);
    } else {
      String base = baseBranch == null ? Constants.MASTER : baseBranch;
      Ref master = repository.findRef(Constants.R_HEADS + base);
      ObjectId masterTip = master.getObjectId();
      RefUpdate createBranch = repository.updateRef(Constants.R_HEADS + branch);
      createBranch.setNewObjectId(masterTip);
      Result result = createBranch.update();
      if (!result.equals(Result.NEW)) {
        throw new IllegalStateException(
          "result did not complete for create branch on " + branch + " with state " + result);
      }
      headVersion = new HeadVersionPair(masterTip.name(), 0L);
    }
    return new VersionedStringWrapper<Branch>(
      null,
      headVersion.getVersion(),
      headVersion.getHead());
  }

  @Override
  public List<Branch> getBranches() throws IOException {
    return repository.getRefDatabase()
                     .getRefsByPrefix(Constants.R_HEADS)
                     .stream()
                     .map(JGitContainerImpl::fromRef)
                     .collect(Collectors.toList());

  }

  @Override
  public VersionedStringWrapper<Branch> getBranch(String branch) throws IOException {
    Ref ref = repository.findRef(Constants.R_HEADS + branch);
    if (ref == null) {
      return null;
    }
    OptionalLong version = ((NessieObjDatabase) repository.getObjectDatabase())
        .getVersion(ref.getName());
    String commitId = ref.getObjectId().name();
    return new VersionedStringWrapper<>(fromRef(ref), version.orElse(-1), commitId);
  }

  @Override
  public VersionedStringWrapper<BranchTable> getTable(String branch, String table)
      throws IOException {
    try (TreeWalk treeWalk = new TreeWalk(repository)) {
      ObjectId treeId = repository.resolve(branch + "^{tree}");
      treeWalk.addTree(treeId);
      treeWalk.setRecursive(true);
      treeWalk.setFilter(PathFilter.create(directoryFor(table)));
      while (treeWalk.next()) {
        ObjectId objectId = treeWalk.getObjectId(0);
        ObjectLoader loader = repository.open(objectId);
        BranchTable branchTable = ProtoUtil.tableFromBytes(loader.getBytes());
        VersionedStringWrapper<Branch> branchObj = getBranch(branch);
        return new VersionedStringWrapper<>(branchTable,
                                            branchObj.getVersion().orElse(-1),
                                            branchObj.getVersionString());
      }
    }
    return null;
  }

  private static ObjectId idFor(String filename) {
    SHA1 md = SHA1.newInstance();
    md.update(filename.getBytes(StandardCharsets.US_ASCII));
    return md.toObjectId();
  }

  private static String directoryFor(String tableName) {
    Map.Entry<String, String> pair = fileFolderFor(tableName);
    return SLASH.join(pair.getKey(), pair.getValue());
  }

  private static Map.Entry<String, String> fileFolderFor(String tableName) {
    ObjectId fileId = idFor(tableName);
    String object = fileId.name();
    return new AbstractMap.SimpleImmutableEntry<>(object.substring(0, 2), object.substring(2));
  }

  private Map<String, Map<String, ObjectId>> commitObjects(BranchTable[] tables)
      throws IOException {
    ObjectInserter inserter = repository.newObjectInserter();
    Map<String, Map<String, ObjectId>> commits = new HashMap<>();
    for (BranchTable branchTable : tables) {
      byte[] data = ProtoUtil.tableToProtoc(branchTable).toByteArray();
      ObjectId blobId = branchTable.isDeleted() ? null : inserter.insert(Constants.OBJ_BLOB, data);
      Map.Entry<String, String> pair = fileFolderFor(branchTable.getId());
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
    while (treeWalk.next() && treeWalk.getDepth() == depth) {
      String path = treeWalk.getPathString();
      String filename = path.replaceFirst(folder + "/", "");
      if (filenames.containsKey(filename)) {
        ObjectId blobId = filenames.remove(filename);
        if (blobId != null) {
          subTreeFormatter.append(filename, FileMode.REGULAR_FILE, blobId);
        }
      } else {
        subTreeFormatter.append(filename, FileMode.REGULAR_FILE, treeWalk.getObjectId(0));
      }
    }
    if (!filenames.isEmpty()) {
      filenames.forEach(
          (destinationFilename, blobId) -> subTreeFormatter.append(destinationFilename,
                                                                   FileMode.REGULAR_FILE,
                                                                   blobId));
      filenames.clear();
    }
    ObjectId subTreeId = inserter.insert(subTreeFormatter);
    inserter.flush();
    return subTreeId;
  }

  private ObjectId newSubTree(Map<String, ObjectId> filenames)
      throws IOException {
    ObjectInserter inserter = repository.newObjectInserter();
    TreeFormatter subTreeFormatter = new TreeFormatter();
    boolean added = false;
    for (Map.Entry<String, ObjectId> entry : filenames.entrySet()) {
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
  public VersionedStringWrapper<BranchTable> commit(String branch,
                                                    Principal principal,
                                                    HeadVersionPair version,
                                                    BranchTable[] tables) throws IOException {
    ObjectId treeId = repository.resolve(branch + "^{tree}");
    TreeWalk treeWalk = new TreeWalk(repository);
    treeWalk.addTree(treeId);
    treeWalk.setRecursive(false);
    TreeFormatter treeFormatter = new TreeFormatter();

    Map<String, Map<String, ObjectId>> commits = commitObjects(tables);
    while (treeWalk.next()) {
      String folder = treeWalk.getPathString();
      if (commits.containsKey(folder)) {
        Map<String, ObjectId> filenames = commits.remove(folder);
        ObjectId subTreeId = commitSubtree(treeWalk, folder, filenames);
        treeFormatter.append(folder, FileMode.TREE, subTreeId);
      } else {
        treeFormatter.append(folder, FileMode.TREE, treeWalk.getObjectId(0));
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
    Optional<Long> updateTime = Arrays.stream(tables)
                                      .map(BranchTable::getUpdateTime)
                                      .max(Long::compareTo);
    HeadVersionPair versionHead = commitTree(treeFormatter,
                                             branch,
                                             updateTime.orElse(Long.MIN_VALUE),
                                             principal,
                                             version);
    return new VersionedStringWrapper<BranchTable>(null,
                                                   versionHead.getVersion(),
                                                   versionHead.getHead());
  }

  @Override
  public void deleteBranch(String branch,
                           HeadVersionPair ifMatch,
                           boolean purge) throws IOException {
    RefUpdate update = repository.updateRef(Constants.R_HEADS + branch);
    if (!ObjectId.isEqual(update.getRef().getObjectId(), ObjectId.fromString(ifMatch.getHead()))) {
      throw new IllegalStateException("can't delete branch, not HEAD");
    }
    update.setRefLogMessage("branch deleted", false);
    update.setForceUpdate(true);
    Result deleteResult = update.delete();  // todo concurrency & check
    if (!deleteResult.equals(Result.FORCED)) {
      throw new IOException("delete failed " + deleteResult);
    }
  }

  @Override
  public List<String> getTables(String branch, String namespace) throws IOException {
    List<String> tables = new ArrayList<>();
    try (TreeWalk treeWalk = new TreeWalk(repository)) {
      ObjectId treeId = repository.resolve(branch + "^{tree}");
      treeWalk.addTree(treeId);
      treeWalk.setRecursive(true);
      while (treeWalk.next()) {
        ObjectId objectId = treeWalk.getObjectId(0);
        ObjectLoader loader = repository.open(objectId);
        BranchTable branchTable = ProtoUtil.tableFromBytes(loader.getBytes());
        if (namespace != null && !namespace.equals(branchTable.getNamespace())) {
          continue;
        }
        tables.add(branchTable.getId());
      }
    }
    return tables;
  }

  @Override
  public HeadVersionPair promote(String branch,
                                 String mergeBranch,
                                 HeadVersionPair version)
      throws IOException {
    ObjectId newCommitId = repository.resolve(mergeBranch + "^{commit}");
    return updateRef(branch, newCommitId, version);
  }

  private HeadVersionPair commitTree(TreeFormatter treeFormatter,
                                     String branch,
                                     long updateTime,
                                     Principal principal,
                                     HeadVersionPair version) throws IOException {
    ObjectId commitId;
    try {
      commitId = repository.resolve(branch + "^{commit}");
    } catch (IOException e) {
      commitId = null;
    }
    if (version != null
        && version.getHead() != null
        && !ObjectId.isEqual(commitId, ObjectId.fromString(version.getHead()))) {
      throw new IllegalStateException("version commit doesn't equal current HEAD");
    }
    ObjectInserter inserter = repository.newObjectInserter();
    ObjectId newTreeId = inserter.insert(treeFormatter);
    inserter.flush();
    CommitBuilder commitBuilder = fromUser(principal, updateTime);
    commitBuilder.setTreeId(newTreeId);
    if (commitId != null) {
      commitBuilder.addParentId(commitId);
    }
    ObjectId newCommitId = inserter.insert(commitBuilder);
    inserter.flush();
    return updateRef(branch, newCommitId, version);
  }

  private HeadVersionPair updateRef(String branch, ObjectId newCommitId, HeadVersionPair version)
      throws IOException {
    RefUpdate ref = repository.updateRef("refs/heads/" + branch);
    Long versionNumber = version != null ? version.getVersion() : null;
    ((NessieRefDatabase) repository.getRefDatabase()).setVersion(ref.getRef(), versionNumber);
    ref.setNewObjectId(newCommitId);
    Result result = ref.update();
    if (!result.equals(Result.NEW) && !result.equals(Result.FAST_FORWARD)) {
      throw new IllegalStateException(
        "Unable to complete commit and update Ref " + ref.getRef() + ", result was " + result);
    }
    OptionalLong newVersion = ((NessieObjDatabase) repository.getObjectDatabase())
        .getVersion(ref.getName());
    return new HeadVersionPair(newCommitId.name(), newVersion.orElse(-1L));
  }

  private static CommitBuilder fromUser(Principal principal, long now) {
    CommitBuilder commitBuilder = new CommitBuilder();
    commitBuilder.setMessage("placeholder");
    PersonIdent person = new PersonIdent("test", "me@example.com");
    if (principal != null) {
      person = new PersonIdent(principal.getName(),
                               "me@example.com",
                               now,
                               0);
    }
    commitBuilder.setAuthor(person);
    commitBuilder.setCommitter(person);
    return commitBuilder;
  }

  public static Branch fromRef(Ref ref) {
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
}

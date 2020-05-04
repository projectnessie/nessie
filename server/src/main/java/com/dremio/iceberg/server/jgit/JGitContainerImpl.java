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

package com.dremio.iceberg.server.jgit;

import com.dremio.iceberg.backend.Backend;
import com.dremio.iceberg.model.Branch;
import com.dremio.iceberg.model.BranchTable;
import com.dremio.iceberg.model.ImmutableBranchTable;
import com.dremio.iceberg.model.VersionedWrapper;
import com.google.common.base.Joiner;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.Principal;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.stream.Collectors;
import javax.inject.Inject;
import org.apache.commons.lang3.tuple.Pair;
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
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;

public class JGitContainerImpl implements JGitContainer {

  private static final Joiner DOT = Joiner.on('.');
  private static final Joiner SLASH = Joiner.on("/");

  private final Repository repository;

  @Inject
  public JGitContainerImpl(Backend backend) {
    DfsRepositoryDescription repoDesc = new DfsRepositoryDescription();
    Repository repository;
    try {
      repository = new NessieRepository.Builder().setRepositoryDescription(repoDesc)
                                                 .setBackend(backend.gitBackend()).build();
    } catch (IOException e) {
      //pass can't happen
      repository = null;
    }
    this.repository = repository;
  }

  @Override
  public void create(String branch, String baseBranch, Principal principal) throws IOException {
    if (branch.equals("master") && baseBranch == null) {
      TreeFormatter formatter = new TreeFormatter();
      long updateTime = ZonedDateTime.now(ZoneId.of("UTC")).toInstant().toEpochMilli();
      commitTree(formatter, branch, updateTime, principal, null);
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
    }

  }

  @Override
  public List<Branch> getBranches() throws IOException {
    return repository.getRefDatabase()
                     .getRefsByPrefix(Constants.R_HEADS)
                     .stream()
                     .map(Branch::fromRef)
                     .collect(Collectors.toList());

  }

  @Override
  public VersionedWrapper<Branch> getBranch(String branch) throws IOException {
    Ref ref = repository.findRef(branch);
    OptionalLong version = ((NessieObjDatabase) repository.getObjectDatabase())
      .getVersion(ref.getName());
    return new VersionedWrapper<>(Branch.fromRef(ref), version.orElse(-1));
  }

  @Override
  public VersionedWrapper<BranchTable> getTable(String branch, String table) throws IOException {
    try (TreeWalk treeWalk = new TreeWalk(repository)) {
      ObjectId treeId = repository.resolve(branch + "^{tree}");
      treeWalk.addTree(treeId);
      treeWalk.setRecursive(true);
      treeWalk.setFilter(PathFilter.create(directoryFor(table)));
      while (treeWalk.next()) {
        String filename = treeWalk.getPathString();
        ObjectId objectId = treeWalk.getObjectId(0);
        ObjectLoader loader = repository.open(objectId);
        String content = new String(loader.getBytes());
        String[] names = table.split("\\.");
        String namespace = null;
        if (names.length > 1) {
          namespace = DOT.join(Arrays.copyOf(names, names.length - 1));
        }
        String name = names[names.length - 1];
        return new VersionedWrapper<>(ImmutableBranchTable.builder()
                                                          .id(table)
                                                          .tableName(name)
                                                          .namespace(namespace)
                                                          .metadataLocation(content)
                                                          .build(),
                                      getBranch(branch).getVersion().orElse(-1));
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
    Pair<String, String> pair = fileFolderFor(tableName);
    return SLASH.join(pair.getLeft(), pair.getRight());
  }

  private static Pair<String, String> fileFolderFor(String tableName) {
    ObjectId fileId = idFor(tableName);
    String object = fileId.name();
    return Pair.of(object.substring(0,2), object.substring(2));
  }

  private Map<String, Map<String, ObjectId>> commitObjects(BranchTable[] tables)
    throws IOException {
    ObjectInserter inserter = repository.newObjectInserter();
    Map<String, Map<String, ObjectId>> commits = new HashMap<>();
    for (BranchTable branchTable : tables) {
      ObjectId blobId = inserter.insert(Constants.OBJ_BLOB,
                                        branchTable.getMetadataLocation().getBytes());
      Pair<String, String> pair = fileFolderFor(branchTable.getId());
      String destinationFolder = pair.getLeft();
      String destinationFilename = pair.getRight();
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
    ObjectInserter inserter = repository.newObjectInserter();
    treeWalk.enterSubtree();
    int depth = treeWalk.getDepth();
    TreeFormatter subTreeFormatter = new TreeFormatter();
    while (treeWalk.next() && treeWalk.getDepth() == depth) {
      String path = treeWalk.getPathString();
      String filename = path.replaceFirst(folder + "/", "");
      if (filenames.containsKey(filename)) {
        ObjectId blobId = filenames.remove(filename);
        subTreeFormatter.append(filename, FileMode.REGULAR_FILE, blobId);
      } else {
        subTreeFormatter.append(filename, FileMode.REGULAR_FILE, treeWalk.getObjectId(0));
      }
    }
    if (!filenames.isEmpty()) {
      filenames.forEach((destinationFilename, blobId) -> subTreeFormatter.append(destinationFilename,
                                                                                 FileMode.REGULAR_FILE,
                                                                                 blobId));
      filenames.clear();
    }
    inserter.flush();
    ObjectId subTreeId = inserter.insert(subTreeFormatter);
    inserter.flush();
    return subTreeId;
  }

  private ObjectId newSubTree(Map<String, ObjectId> filenames)
    throws IOException {
    ObjectInserter inserter = repository.newObjectInserter();
    TreeFormatter subTreeFormatter = new TreeFormatter();
    filenames.forEach((destinationFilename, objectId) -> subTreeFormatter.append(destinationFilename,
                                                                                 FileMode.REGULAR_FILE,
                                                                                 objectId));
    filenames.clear();
    inserter.flush();
    ObjectId subTreeId = inserter.insert(subTreeFormatter);
    inserter.flush();
    return subTreeId;
  }

  @Override
  public void commit(String branch,
                     Principal principal,
                     Long version,
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
        treeFormatter.append(destinationFolder, FileMode.TREE, subTreeId);
      }
      commits.clear();
    }
    Optional<Long> updateTime = Arrays.stream(tables)
                                      .map(BranchTable::getUpdateTime)
                                      .max(Long::compareTo);
    commitTree(treeFormatter, branch, updateTime.orElse(Long.MIN_VALUE), principal, version);
  }

  private void commitTree(TreeFormatter treeFormatter,
                          String branch,
                          long updateTime,
                          Principal principal,
                          Long version) throws IOException {
    ObjectId commitId;
    try {
      commitId = repository.resolve(branch + "^{commit}");
    } catch (IOException e) {
      commitId = null;
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
    RefUpdate ref = repository.updateRef("refs/heads/" + branch);
    ((NessieRefDatabase)repository.getRefDatabase()).setVersion(ref.getRef(), version);
    ref.setNewObjectId(newCommitId);
    Result result = ref.update();
    if (!result.equals(Result.NEW) && !result.equals(Result.FAST_FORWARD)) {
      throw new IllegalStateException(
        "Unable to complete commit and update Ref " + ref.getRef() + ", result was " + result);
    }
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
}

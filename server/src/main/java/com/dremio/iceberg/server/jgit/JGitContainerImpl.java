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
import com.google.common.base.Joiner;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.inject.Inject;
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

public class JGitContainerImpl implements JGitContainer {
  private static final Joiner DOT = Joiner.on('.');

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
  public void add(String branch, Map<String, Map<String, String>> tables) throws Throwable {
    ObjectInserter inserter = repository.newObjectInserter();
    TreeFormatter treeFormatter = new TreeFormatter();
    for (Map.Entry<String, Map<String, String>> subtree : tables.entrySet()) {
      TreeFormatter subTreeFormatter = new TreeFormatter();
      for (Map.Entry<String, String> entry : subtree.getValue().entrySet()) {
        String path = entry.getKey();
        byte[] blob = entry.getValue().getBytes();
        ObjectId blobId = inserter.insert(Constants.OBJ_BLOB, blob);
        subTreeFormatter.append(path, FileMode.REGULAR_FILE, blobId);
      }
      inserter.flush();
      ObjectId subtreeId = inserter.insert(subTreeFormatter);
      inserter.flush();
      treeFormatter.append(subtree.getKey(), FileMode.TREE, subtreeId);
    }
    inserter.flush();
    ObjectId treeId = inserter.insert(treeFormatter);
    inserter.flush();
    CommitBuilder commitBuilder = new CommitBuilder();
    commitBuilder.setTreeId(treeId);
    commitBuilder.setMessage("My first commit!");
    PersonIdent person = new PersonIdent("me", "me@example.com");
    commitBuilder.setAuthor(person);
    commitBuilder.setCommitter(person);
    ObjectId commitId = inserter.insert(commitBuilder);
    inserter.flush();
    RefUpdate ref = repository.updateRef("refs/heads/" + branch);
    ref.setNewObjectId(commitId);
    Result result = ref.update();
  }

  @Override
  @SuppressWarnings("VariableDeclarationUsageDistance")
  public void update(String branch, Map<String, Map<String, String>> tables) throws Throwable {
    ObjectId treeId = repository.resolve(branch + "^{tree}");
    ObjectId commitId = repository.resolve(branch + "^{commit}");
    TreeWalk treeWalk = new TreeWalk(repository);
    treeWalk.addTree(treeId);
    treeWalk.setRecursive(false);
    TreeFormatter treeFormatter = new TreeFormatter();
    while (treeWalk.next()) {
      String folder = treeWalk.getPathString();
      if (tables.containsKey(folder)) {
        treeWalk.enterSubtree();
        int depth = treeWalk.getDepth();
        ObjectInserter inserter = repository.newObjectInserter();
        TreeFormatter subTreeFormatter = new TreeFormatter();
        Map<String, String> updates = tables.get(folder);
        while (treeWalk.next() && treeWalk.getDepth() == depth) {
          String path = treeWalk.getPathString();
          String filename = path.replaceFirst(folder + "/", "");
          if (updates.containsKey(filename)) {
            String item = updates.remove(filename);
            ObjectId blobId = inserter.insert(Constants.OBJ_BLOB, item.getBytes());
            subTreeFormatter.append(filename, FileMode.REGULAR_FILE, blobId);
          } else {
            subTreeFormatter.append(filename, FileMode.REGULAR_FILE, treeWalk.getObjectId(0));
          }
        }
        for (Map.Entry<String, String> entry : updates.entrySet()) {
          ObjectId blobId = inserter.insert(Constants.OBJ_BLOB, entry.getValue().getBytes());
          subTreeFormatter.append(entry.getKey(), FileMode.REGULAR_FILE, blobId);
        }
        inserter.flush();
        ObjectId subTreeId = inserter.insert(subTreeFormatter);
        inserter.flush();
        treeFormatter.append(folder, FileMode.TREE, subTreeId);
      } else {
        treeFormatter.append(folder, FileMode.TREE, treeWalk.getObjectId(0));
      }
    }
    ObjectInserter inserter = repository.newObjectInserter();
    inserter.flush();
    ObjectId newTreeId = inserter.insert(treeFormatter);
    inserter.flush();
    CommitBuilder commitBuilder = new CommitBuilder();
    commitBuilder.setTreeId(newTreeId);
    commitBuilder.setMessage("My second commit!");
    PersonIdent person = new PersonIdent("me", "me@example.com");
    commitBuilder.setAuthor(person);
    commitBuilder.setCommitter(person);
    commitBuilder.addParentId(commitId);
    ObjectId newCommitId = inserter.insert(commitBuilder);
    inserter.flush();
    RefUpdate ref = repository.updateRef("refs/heads/" + branch);
    ref.setNewObjectId(newCommitId);
    Result result = ref.update();
  }

  @Override
  public void read(String branch, String table) throws Throwable {
    ObjectId treeId = repository.resolve(branch + "^{tree}");
    TreeWalk treeWalk = new TreeWalk(repository);
    treeWalk.addTree(treeId);
    treeWalk.setRecursive(true);
    if (table != null) {
      treeWalk.setFilter(PathFilter.create(table));
    }
    String filename = "";
    String content = "";
    while (treeWalk.next()) {
      filename = treeWalk.getPathString();
      ObjectId objectId = treeWalk.getObjectId(0);
      ObjectLoader loader = repository.open(objectId);
      content = new String(loader.getBytes());
    }
  }

  public static void main(String[] args) throws Throwable {
    /*JGitContainer container = new JGitContainerImpl(null);
    container.create("master");
    container.create("test");
    Map<String, Map<String, String>> files = new HashMap<>();
    int a = 10;
    int b = 10;
    for (int i = 0; i < a; i++) {
      int finalI = i;
      files.put("folder" + i, IntStream.range(0, b)
                                       .mapToObj(j -> "table" + (finalI * b + j))
                                       .collect(Collectors.toMap(
                                         x -> x,
                                         x -> "/path/to/table/metadata" + x
                                       )));
    }
    container.add("test", files);
    container.read("test", "folder2/table2203");
    Map<String, Map<String, String>> updates = new HashMap<>();
    updates.put("folder1", new HashMap<>());
    updates.put("folder2", new HashMap<>());
    updates.get("folder1").put("tablexxx", "/path/to/table/metadatdataxxx");
    updates.get("folder2").put("table2203", "/path/to/table/2/metadatdata2203");
    container.update("test", updates);
    container.read("test", "folder1/tablexxx");
    container.read("test", "folder2/table2203");
    System.out.println("foo");*/
  }

  @Override
  public void create(String branch) throws IOException {
    if (branch.equals("master")) {
      ObjectInserter inserter = repository.newObjectInserter();
      TreeFormatter formatter = new TreeFormatter();
      ObjectId treeId = formatter.insertTo(inserter);
      CommitBuilder commitBuilder = new CommitBuilder();
      commitBuilder.setTreeId(treeId);
      commitBuilder.setMessage("My second commit!");
      PersonIdent person = new PersonIdent("me", "me@example.com");
      commitBuilder.setAuthor(person);
      commitBuilder.setCommitter(person);
      ObjectId newCommitId = inserter.insert(commitBuilder);
      inserter.flush();
      RefUpdate ref = repository.updateRef("refs/heads/" + branch);
      ref.setNewObjectId(newCommitId);
      Result result = ref.update();
    } else {
      Ref master = repository.findRef(Constants.R_HEADS + Constants.MASTER);
      ObjectId masterTip = master.getObjectId();
      RefUpdate createBranch = repository.updateRef(Constants.R_HEADS + branch);
      createBranch.setNewObjectId(masterTip);
      createBranch.update();
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
  public Branch getBranch(String branch) throws IOException {
    return Branch.fromRef(repository.findRef(branch));
  }

  @Override
  public BranchTable getTable(String branch, String table) throws IOException {
    try (TreeWalk treeWalk = new TreeWalk(repository)) {
      ObjectId treeId = repository.resolve(branch + "^{tree}");
      treeWalk.addTree(treeId);
      treeWalk.setRecursive(true);
      if (table != null) {
        treeWalk.setFilter(PathFilter.create(table));
      }
      while (treeWalk.next()) {
        String filename = treeWalk.getPathString();
        ObjectId objectId = treeWalk.getObjectId(0);
        ObjectLoader loader = repository.open(objectId);
        String content = new String(loader.getBytes());
        String[] names = filename.split("\\.");
        String namespace = null;
        if (names.length > 1) {
          namespace = DOT.join(Arrays.copyOf(names, names.length - 1));
        }
        String name = names[names.length - 1];
        return ImmutableBranchTable.builder()
          .id(filename)
          .tableName(name)
          .namespace(namespace)
          .metadataLocation(content)
          .build();
      }
    }
    return null;
  }

  @Override
  public void commit(String branch, String table, BranchTable branchTable) throws IOException {
    ObjectId treeId = repository.resolve(branch + "^{tree}");
    ObjectId commitId = repository.resolve(branch + "^{commit}");
    TreeWalk treeWalk = new TreeWalk(repository);
    treeWalk.addTree(treeId);
    treeWalk.setRecursive(false);
    TreeFormatter treeFormatter = new TreeFormatter();
    ObjectInserter inserter = repository.newObjectInserter();
    ObjectId blobId = inserter.insert(Constants.OBJ_BLOB, branchTable.getMetadataLocation().getBytes());
    String destinationFolder = Integer.toString(blobId.getFirstByte());
    String destinationFilename = blobId.name().replace(destinationFolder, "");
    boolean folderFound = false;
    boolean fileFound = false;
    while (treeWalk.next()) {
      String folder = treeWalk.getPathString();
      if (destinationFolder.equals(folder)) {
        folderFound = true;
        treeWalk.enterSubtree();
        int depth = treeWalk.getDepth();
        TreeFormatter subTreeFormatter = new TreeFormatter();
        while (treeWalk.next() && treeWalk.getDepth() == depth) {
          String path = treeWalk.getPathString();
          String filename = path.replaceFirst(folder + "/", "");
          if (destinationFilename.equals(filename)) {
            subTreeFormatter.append(filename, FileMode.REGULAR_FILE, blobId);
            fileFound = true;
          } else {
            subTreeFormatter.append(filename, FileMode.REGULAR_FILE, treeWalk.getObjectId(0));
          }
        }
        if (!fileFound) {
          subTreeFormatter.append(destinationFilename, FileMode.REGULAR_FILE, blobId);
          fileFound = true;
        }
        inserter.flush();
        ObjectId subTreeId = inserter.insert(subTreeFormatter);
        inserter.flush();
        treeFormatter.append(folder, FileMode.TREE, subTreeId);
      } else {
        treeFormatter.append(folder, FileMode.TREE, treeWalk.getObjectId(0));
      }
    }
    if (!folderFound) {
      TreeFormatter subTreeFormatter = new TreeFormatter();
      subTreeFormatter.append(destinationFilename, FileMode.REGULAR_FILE, blobId);
      fileFound = true;
      inserter.flush();
      ObjectId subTreeId = inserter.insert(subTreeFormatter);
      inserter.flush();
      treeFormatter.append(destinationFolder, FileMode.TREE, subTreeId);
      folderFound = true;
    }
    assert fileFound && folderFound;
    inserter.flush();
    ObjectId newTreeId = inserter.insert(treeFormatter);
    inserter.flush();
    CommitBuilder commitBuilder = new CommitBuilder();
    commitBuilder.setTreeId(newTreeId);
    commitBuilder.setMessage("My second commit!");
    PersonIdent person = new PersonIdent("me", "me@example.com");
    commitBuilder.setAuthor(person);
    commitBuilder.setCommitter(person);
    commitBuilder.addParentId(commitId);
    ObjectId newCommitId = inserter.insert(commitBuilder);
    inserter.flush();
    RefUpdate ref = repository.updateRef("refs/heads/" + branch);
    ref.setNewObjectId(newCommitId);
    Result result = ref.update();
  }

}

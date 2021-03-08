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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.eclipse.jgit.dircache.DirCache;
import org.eclipse.jgit.dircache.DirCacheBuilder;
import org.eclipse.jgit.dircache.DirCacheEntry;
import org.eclipse.jgit.lib.Constants;
import org.eclipse.jgit.lib.FileMode;
import org.eclipse.jgit.lib.ObjectId;
import org.eclipse.jgit.lib.ObjectInserter;
import org.eclipse.jgit.lib.Repository;
import org.eclipse.jgit.revwalk.RevCommit;
import org.eclipse.jgit.treewalk.NameConflictTreeWalk;
import org.eclipse.jgit.treewalk.TreeWalk;
import org.projectnessie.versioned.Delete;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.Operation;
import org.projectnessie.versioned.Put;
import org.projectnessie.versioned.ReferenceNotFoundException;
import org.projectnessie.versioned.SerializerWithPayload;
import org.projectnessie.versioned.Unchanged;

public class TreeBuilder {

  /**
   * Turn a list of operations into a Git Tree.
   *
   * @param ops list of operations
   * @param repository repository to commit into
   * @param serializer how to turn a TABLE into byte[]
   * @param <TABLE> type of data stored in jgit
   * @return objectId of new tree
   * @throws IOException error in speaking w/ git
   */
  public static <TABLE> ObjectId commitObjects(List<Operation<TABLE>> ops,
                                               Repository repository,
                                               SerializerWithPayload<TABLE> serializer,
                                               ObjectId emptyObjectId) throws IOException {
    ObjectInserter inserter = repository.newObjectInserter();

    DirCache dc = DirCache.newInCore();

    DirCacheBuilder builder = dc.builder();
    for (Operation<TABLE> op: ops) {
      final ObjectId objectId;
      final FileMode fileMode;
      if (op instanceof Unchanged) {
        continue;
      } else if (op instanceof Delete) {
        /*
        using an empty object and a symlink as a sentinel value for deleted files.
        This is required as we have to be able to tell the difference between 'deleted' and 'not changed in this commit' when merging
        this tree with the tree on HEAD.
         */
        objectId = emptyObjectId;
        fileMode = FileMode.GITLINK;
      } else if (op instanceof Put) {
        byte[] data = serializer.toBytes(((Put<TABLE>) op).getValue()).toByteArray();
        objectId = inserter.insert(Constants.OBJ_BLOB, data);
        fileMode = FileMode.REGULAR_FILE;
      } else {
        throw new IllegalStateException(String.format("unknown operation type %s", op));
      }
      String name = JGitVersionStore.stringFromKey(op.getKey());
      DirCacheEntry dce = new DirCacheEntry(name);
      dce.setObjectId(objectId);
      dce.setFileMode(fileMode);
      builder.add(dce);
    }
    builder.finish();
    ObjectId objectId = dc.writeTree(inserter);
    inserter.flush();
    return objectId;
  }

  /**
   * Merge a newly created tree into the current tree. The new tree always wins
   *
   * @param currentTree the current HEAD tree
   * @param newTree new tree of changes
   * @param repository repository to store in
   * @return merged tree id
   * @throws IOException if there are problems talking to git
   */
  public static ObjectId merge(ObjectId currentTree, ObjectId newTree, Repository repository)
      throws IOException {
    TreeWalk treeWalk = new NameConflictTreeWalk(repository);
    treeWalk.addTree(currentTree);
    treeWalk.addTree(newTree);
    treeWalk.setRecursive(false);
    DirCache dc = DirCache.newInCore();
    DirCacheBuilder builder = dc.builder();
    diff(treeWalk, builder);
    builder.finish();
    ObjectInserter inserter = repository.newObjectInserter();
    ObjectId objectId = dc.writeTree(inserter);
    inserter.flush();
    return objectId;
  }

  /**
   * turn a set of commit hashes into an aggregate tree.
   */
  public static ObjectId transplant(Hash hash, Repository repository) throws IOException, ReferenceNotFoundException {

    ObjectId treeId = repository.resolve(hash.asString() + "^{tree}");
    if (treeId == null) {
      throw ReferenceNotFoundException.forReference(hash);
    }
    RevCommit commit = RevCommit.parse(repository.open(repository.resolve(hash.asString() + "^{commit}")).getBytes());
    String parentTreeName = commit.getParent(0).name();
    ObjectId parentTree = repository.resolve(parentTreeName + "^{tree}");
    return merge(parentTree, treeId, repository);
  }

  private static void diff(TreeWalk treeWalk, DirCacheBuilder builder) throws IOException {
    while (treeWalk.next()) {
      ObjectId current = treeWalk.getObjectId(0);
      ObjectId next = treeWalk.getObjectId(1);
      if (treeWalk.isSubtree()) {
        if (ObjectId.isEqual(next, ObjectId.zeroId())) {
          builder.addTree(treeWalk.getRawPath(), 0, treeWalk.getObjectReader(), treeWalk.getObjectId(0));
        } else if (ObjectId.isEqual(current, ObjectId.zeroId())) {
          builder.addTree(treeWalk.getRawPath(), 0, treeWalk.getObjectReader(), treeWalk.getObjectId(1));
        } else if (!treeWalk.idEqual(0, 1)) {
          treeWalk.enterSubtree();
        }
      } else {
        if (ObjectId.isEqual(next, ObjectId.zeroId()) && treeWalk.idEqual(0, 1)) {
          continue;
        } else if (ObjectId.isEqual(next, ObjectId.zeroId())) {
          DirCacheEntry dce = new DirCacheEntry(treeWalk.getRawPath());
          dce.setObjectId(treeWalk.getObjectId(0));
          dce.setFileMode(treeWalk.getFileMode(0));
          builder.add(dce);
        } else if (ObjectId.isEqual(current, ObjectId.zeroId()) || !treeWalk.idEqual(0, 1)) {
          DirCacheEntry dce = new DirCacheEntry(treeWalk.getRawPath());
          if (treeWalk.getFileMode(1).equals(FileMode.GITLINK)) {
            dce.setObjectId(ObjectId.zeroId());
          } else {
            dce.setObjectId(treeWalk.getObjectId(1));
          }
          dce.setFileMode(treeWalk.getFileMode(1));
          builder.add(dce);
        }
      }
    }
  }

  private static List<String> testRead(ObjectId objectId, Repository repository) throws IOException {
    TreeWalk tw = new TreeWalk(repository);
    tw.setRecursive(true);
    tw.addTree(objectId);
    List<String> fields = new ArrayList<>();
    while (tw.next()) {
      fields.add(tw.getPathString());
    }
    return fields;
  }

  private static Map<String, String> testReadAll(ObjectId objectId, Repository repository) throws IOException {
    TreeWalk tw = new TreeWalk(repository);
    tw.setRecursive(true);
    tw.addTree(objectId);
    Map<String, String> fields = new HashMap<>();
    while (tw.next()) {
      fields.put(tw.getPathString(), new String(repository.open(tw.getObjectId(0)).getBytes()));
    }
    return fields;
  }
}

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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.eclipse.jgit.dircache.DirCache;
import org.eclipse.jgit.dircache.DirCacheBuilder;
import org.eclipse.jgit.dircache.DirCacheEntry;
import org.eclipse.jgit.errors.UnmergedPathException;
import org.eclipse.jgit.lib.FileMode;
import org.eclipse.jgit.lib.ObjectId;
import org.eclipse.jgit.lib.ObjectInserter;
import org.eclipse.jgit.lib.Repository;
import org.eclipse.jgit.merge.ThreeWayMerger;
import org.eclipse.jgit.treewalk.AbstractTreeIterator;
import org.eclipse.jgit.treewalk.NameConflictTreeWalk;

/**
 * Simple merge of two potentially conflicting branches.
 *
 * <p>If no file level conflicts exist the merge will succeed. Any file level merges will result in
 * failure. Very similar to StrategySimpleTwoWayInCore but contains extra info for handling Nessie
 * conflicts.
 */
class TwoWayMerger extends ThreeWayMerger {

  private static final int T_BASE = 0;

  private static final int T_OURS = 1;

  private static final int T_THEIRS = 2;

  private final NameConflictTreeWalk tw;
  private final Set<String> unchanged;

  private final DirCache cache;

  private DirCacheBuilder builder;

  private ObjectId resultTree;

  TwoWayMerger(Repository local, List<String> unchanged) {
    super(local);
    tw = new NameConflictTreeWalk(local, reader);
    this.unchanged = new HashSet<>(unchanged);
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

      if (tw.idEqual(T_OURS, T_THEIRS) && tw.idEqual(T_BASE, T_OURS)) {
        add(T_OURS, DirCacheEntry.STAGE_0); // this has not changed at all (tree or otherwise)
        continue;
      }
      if (unchanged.contains(tw.getPathString()) && !tw.idEqual(T_BASE, T_OURS)) {
        add(T_BASE, DirCacheEntry.STAGE_1);
        hasConflict = true;
        continue;
      }
      if (tw.isSubtree()) {
        tw.enterSubtree(); // this changed and is a tree, descend
        continue;
      }
      if (!tw.idEqual(T_OURS, T_THEIRS)) {
        // has changed in this commit
        if (tw.idEqual(T_BASE, T_OURS)) {
          // base and ours are the same, so only changed in this commit. No conflict
          if (ObjectId.zeroId().equals(tw.getObjectId(T_THEIRS))
              && !FileMode.GITLINK.equals(tw.getRawMode(T_THEIRS))) {
            add(T_OURS, DirCacheEntry.STAGE_0);
          } else {
            add(T_THEIRS, DirCacheEntry.STAGE_0);
          }
        } else if (tw.idEqual(T_BASE, T_THEIRS)) {
          // base and theirs are the same, so only changed in other commit. No conflict
          add(T_OURS, DirCacheEntry.STAGE_0);
        } else {
          // all three are different, conflict
          add(T_BASE, DirCacheEntry.STAGE_1);
          hasConflict = true;
        }
      } else {
        // ours and theirs are the same but are different from base...prob a dup change, still a
        // conflict
        add(T_BASE, DirCacheEntry.STAGE_1);
        hasConflict = true;
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

  private void add(int tree, int stage) throws IOException {
    final AbstractTreeIterator i = getTree(tree);
    if (i != null) {
      if (FileMode.TREE.equals(tw.getRawMode(tree))) {
        builder.addTree(tw.getRawPath(), stage, reader, tw.getObjectId(tree));
      } else if (FileMode.GITLINK.equals(tw.getRawMode(tree))) {
        // skip
      } else {
        final DirCacheEntry e = new DirCacheEntry(tw.getRawPath(), stage);
        ;
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

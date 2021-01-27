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
package com.dremio.nessie.versioned.store.rocksdb;

import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import com.dremio.nessie.tiered.builder.Ref;
import com.dremio.nessie.versioned.store.Id;

public class RocksRef extends RocksBaseValue<Ref> implements Ref, Evaluator {

  public static final String TYPE = "type";
  public static final String METADATA = "metadata";
  public static final String COMMITS = "commits";
  public static final String COMMIT = "commit";
  public static final String CHILDREN = "children";

  // TODO: Create correct constuctor with all attributes.
  static RocksRef EMPTY =
      new RocksRef();

  static Id EMPTY_ID = EMPTY.getId();

  RefType type;
  String name;
  Id commit;
  Id metadata;
  Stream<Id> children;
  Consumer<BranchCommitConsumer> commits;

  public RocksRef() {
    super(EMPTY_ID, 0);
  }

  @Override
  public boolean evaluate(Condition condition) {
    // Retrieve entity at function.path
    if (this.type == RefType.BRANCH) {
      // TODO: do we need to subtype?
      RocksBranch internalBranch = (RocksBranch) this;
      for (Function function: condition.functionList) {
        // Branch evaluation
        List<String> path = Arrays.asList(function.getPath().split(Pattern.quote(".")));

        for (String segment : path) {
          if (segment.equals(TYPE)
              && (path.size() == 1)
              && (function.getOperator().equals(Function.EQUALS))
              && (internalBranch.type.toString() != function.getValue().getString())) {
            return false;
          } else if (segment.equals(CHILDREN)) {
            evaluateStream(function, children);
          } else if (segment.equals(METADATA)
              && (path.size() == 1)
              && (function.getOperator().equals(Function.EQUALS))) {
            // TODO: We require a getMetadata() accessor in InternalBranch
            return false;
          } else if (segment.equals(COMMITS)) {
            if (function.getOperator().equals(Function.SIZE)) {
              // Is a List
              // TODO: We require a getCommits() accessor in InternalBranch
              return false;
            } else if (function.getOperator().equals(Function.EQUALS)) {
              // TODO: We require a getCommits() accessor in InternalBranch
            }
          }
        }
      }

      // Tag evaluation
      if (this.type == RefType.TAG) {
        RocksTag rocksTag = (RocksTag) this;
        for (Function function: condition.functionList) {
          // Tag evaluation
          List<String> path = Arrays.asList(function.getPath().split(Pattern.quote(".")));

          for (String segment : path) {
            if (segment.equals(TYPE)
                && (path.size() == 1)
                && (function.getOperator().equals(Function.EQUALS))
                && (rocksTag.type.toString() != function.getValue().getString())) {
              return false;
            } else if (segment.equals(COMMIT)) {
              if (function.getOperator().equals(Function.EQUALS)) {
                if (!rocksTag.commit.equals(function.getValue().getBinary())) {
                  // TODO: We require a getCommit() accessor in internalTag
                  return false;
                }
              }
            }
          }
        }
      }
    }
    return true;
  }

  @Override
  public Ref type(RefType refType) {
    this.type = refType;
    return this;
  }

  @Override
  public Ref name(String name) {
    this.name = name;
    return this;
  }

  @Override
  public Ref commit(Id commit) {
    this.commit = commit;
    return this;
  }

  @Override
  public Ref metadata(Id metadata) {
    this.metadata = metadata;
    return this;
  }

  @Override
  public Ref children(Stream<Id> children) {
    this.children = children;
    return this;
  }

  @Override
  public Ref commits(Consumer<BranchCommitConsumer> commits) {
    this.commits = commits;
    return this;
  }
}

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
package com.dremio.nessie.versioned.gc;

import java.io.Serializable;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

import com.dremio.nessie.tiered.builder.Ref;
import com.dremio.nessie.versioned.Key.Mutation;
import com.dremio.nessie.versioned.store.Id;
import com.dremio.nessie.versioned.store.Store;
import com.dremio.nessie.versioned.store.Store.Acceptor;
import com.dremio.nessie.versioned.store.ValueType;

/**
 * Class used for Branch/Tag records within Spark.
 */
public class RefFrame implements Serializable {
  private static final long serialVersionUID = 2499305773519134099L;

  private String name;
  private IdFrame id;

  /**
   * Construct ref frame.
   */
  public RefFrame(String name, IdFrame id) {
    super();
    this.name = name;
    this.id = id;
  }

  public RefFrame() {
  }

  public static RefFrame of(String name, Id id) {
    return new RefFrame(name, IdFrame.of(id));
  }

  public String getName() {
    return name;
  }

  public IdFrame getId() {
    return id;
  }

  public void setName(String name) {
    this.name = name;
  }

  public void setId(IdFrame id) {
    this.id = id;
  }


  /**
   * For a stream of Refs, convert each to a RefFrame.
   */
  private static final Function<Acceptor<Ref>, RefFrame> CONVERTER = a -> {
    RefFrame frame = new RefFrame();
    a.applyValue(new Ref() {

      private RefType type;

      @Override
      public Ref id(Id id) {
        return this;
      }

      @Override
      public Ref dt(long dt) {
        return this;
      }

      @Override
      public Ref type(RefType refType) {
        return this;
      }

      @Override
      public Ref name(String name) {
        frame.name = name;
        return this;
      }

      @Override
      public Ref commit(Id commit) {
        frame.id = IdFrame.of(commit);
        return this;
      }

      @Override
      public Ref metadata(Id metadata) {
        return this;
      }

      @Override
      public Ref children(Stream<Id> children) {
        return this;
      }

      @Override
      public Ref commits(Consumer<BranchCommitConsumer> commits) {
        commits.accept(new BranchCommitConsumer() {

          private boolean populated;
          private Id id;
          private boolean hasParent;

          @Override
          public BranchCommitConsumer id(Id id) {
            if (!populated) {
              this.id = id;
            }
            return this;
          }

          @Override
          public BranchCommitConsumer commit(Id commit) {
            return this;
          }

          @Override
          public BranchCommitConsumer parent(Id parent) {
            if (!populated) {
              hasParent = true;
            }
            return this;
          }

          @Override
          public BranchCommitConsumer delta(int position, Id oldId, Id newId) {
            return this;
          }

          @Override
          public BranchCommitConsumer keyMutation(Mutation keyMutation) {
            return this;
          }

          @Override
          public BranchCommitConsumer done() {
            if (hasParent) {
              populated = true;
              frame.id = IdFrame.of(id);
            }
            return this;
          }

        });
        return this;
      }
    });

    return frame;

  };

  /**
   * Generate spark dataset from store supplier.
   */
  public static Dataset<RefFrame> asDataset(Supplier<Store> store, SparkSession spark) {
    return ValueRetriever.dataset(
        store,
        ValueType.REF,
        RefFrame.class,
        Optional.empty(),
        spark,
        CONVERTER
        );
  }

}

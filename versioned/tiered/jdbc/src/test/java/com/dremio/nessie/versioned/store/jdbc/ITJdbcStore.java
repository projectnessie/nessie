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
package com.dremio.nessie.versioned.store.jdbc;

import static org.hamcrest.CoreMatchers.anyOf;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Locale;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.hamcrest.Matcher;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import com.dremio.nessie.tiered.builder.Ref;
import com.dremio.nessie.versioned.impl.AbstractTestStore;
import com.dremio.nessie.versioned.store.Id;
import com.dremio.nessie.versioned.store.SaveOp;
import com.dremio.nessie.versioned.store.StoreOperationException;
import com.dremio.nessie.versioned.store.ValueType;

/**
 * Runs basic store-tests against a database in a test-container.
 */
class ITJdbcStore extends AbstractTestStore<JdbcStore> {
  private JdbcFixture fixture;

  @AfterAll
  static void shutdown() {
    JdbcFixture.cleanup();
  }

  @AfterEach
  void deleteResources() {
    if (fixture != null) {
      fixture.close();
    }
  }

  protected JdbcFixture createFixture() {
    return new JdbcOssFixture();
  }

  /**
   * Creates an instance of JdbcStore on which tests are executed.
   * @return the store to test.
   */
  @Override
  protected JdbcStore createStore() {
    fixture = createFixture();
    return fixture.getStore();
  }

  @Override
  protected JdbcStore createRawStore() {
    return createFixture().getStore();
  }

  @Override
  protected int loadSize() {
    return 100;
  }

  @Override
  protected long getRandomSeed() {
    return 8612341233543L;
  }

  @Override
  protected void resetStoreState() {
    super.store = null;
  }

  @Test
  void branchCommitOverflow() {
    Throwable t = assertThrows(StoreOperationException.class, () -> {
      Id id = Id.generateRandom();
      store.putIfAbsent(new SaveOp<Ref>(ValueType.REF, id) {
        @Override
        public void serialize(Ref consumer) {
          consumer.id(id)
              .dt(System.currentTimeMillis() * 1000L)
              .name("branch-commit-overflow")
              .branch()
              .metadata(Id.generateRandom())
              .children(IntStream.range(0, 43).mapToObj(ignore -> Id.generateRandom()))
              .commits(bc -> {
                for (int i = 0; i < JdbcRef.MAX_COMMITS + 1; i++) {
                  bc.saved().parent(Id.generateRandom()).done();
                }
              });
        }
      });
    });
    t = t.getCause();
    assertThat(t, instanceOf(SQLError.class));
    t = t.getCause();
    assertThat(t, instanceOf(SQLException.class));

    // the database/driver should complain that one of the C_* columns does not exist
    Iterable<Matcher<? super String>> matchers = Arrays.stream(JdbcRef.C_COLUMNS)
        .flatMap(c -> Stream.of(containsString(c + "_15"), containsString(c.toUpperCase(Locale.ROOT) + "_15")))
        .collect(Collectors.toList());
    assertThat(t.getMessage(), anyOf(matchers));
  }
}

/*
 * Copyright (C) 2022 Dremio
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
package org.projectnessie.tools.contentgenerator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.projectnessie.tools.contentgenerator.RunContentGenerator.runGeneratorCmd;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.projectnessie.client.api.NessieApiV2;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Branch;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.Reference;
import org.projectnessie.tools.contentgenerator.RunContentGenerator.ProcessResult;

class ITDetachHistory extends AbstractContentGeneratorTest {

  private static final ContentKey key1 = ContentKey.of("test", "key1");
  private static final ContentKey key2 = ContentKey.of("test", "key2");
  private static final ContentKey key3 = ContentKey.of("test_key3");
  private static final IcebergTable table1 = IcebergTable.of("meta_111", 1, 2, 3, 4);
  private static final IcebergTable table2 = IcebergTable.of("meta_222", 1, 2, 3, 4);
  private static final IcebergTable table3 = IcebergTable.of("meta_333", 1, 2, 3, 4);

  private NessieApiV2 api;

  @BeforeEach
  void setUp() {
    api = buildNessieApi();
  }

  private void runMain(String... args) {
    runMain(s -> {}, args);
  }

  private ProcessResult runMain(Consumer<String> stdoutWatcher, String... args) {
    List<String> allArgs = new ArrayList<>();
    allArgs.add("detach-history");
    allArgs.add("--uri");
    allArgs.add(NESSIE_API_URI);
    allArgs.addAll(Arrays.asList(args));

    ProcessResult result = runGeneratorCmd(stdoutWatcher, allArgs.toArray(new String[0]));
    assertThat(result)
        .describedAs(result.toString())
        .extracting(ProcessResult::getExitCode)
        .isEqualTo(0);
    return result;
  }

  private Content contentWithoutId(String ref, ContentKey key) throws NessieNotFoundException {
    return get(api, ref, key).withId(null);
  }

  @Test
  void severalBranchesAllContent() throws NessieNotFoundException, NessieConflictException {
    Branch main = api.getDefaultBranch();
    Branch test =
        (Branch) api.createReference().reference(Branch.of("test1", main.getHash())).create();
    main = create(api, main, table1, key1);
    main = create(api, main, table2, key2);
    test = create(api, test, table1, key1);
    test = create(api, test, table3, key3);

    runMain("--all", "--src-suffix", "-src123", "--final-suffix", "-done123");

    Reference mainOrig = api.getReference().refName(main.getName() + "-src123").get();
    assertThat(mainOrig.getHash()).isEqualTo(main.getHash());
    Reference mainCompleted = api.getReference().refName(main.getName() + "-done123").get();
    assertThat(mainCompleted.getHash()).isEqualTo(main.getHash());
    Reference testOrig = api.getReference().refName(test.getName() + "-src123").get();
    assertThat(testOrig.getHash()).isEqualTo(test.getHash());
    Reference testCompleted = api.getReference().refName(test.getName() + "-done123").get();
    assertThat(testCompleted.getHash()).isEqualTo(test.getHash());

    assertThat(contentWithoutId(main.getName(), key1)).isEqualTo(table1);
    assertThat(contentWithoutId(main.getName(), key2)).isEqualTo(table2);
    assertThat(contentWithoutId(test.getName(), key1)).isEqualTo(table1);
    assertThat(contentWithoutId(test.getName(), key3)).isEqualTo(table3);

    assertThat(contentWithoutId(main.getName(), key1.getParent())).isEqualTo(key1.getNamespace());
  }

  @Test
  void mainBatched() throws NessieNotFoundException, NessieConflictException {
    Branch main = api.getDefaultBranch();
    int numTables = 200;
    for (int i = 1; i < numTables; i++) {
      main = create(api, main, table1, ContentKey.of("test-" + i));
    }

    runMain("--all", "--batch", "7");

    Reference mainOrig = api.getReference().refName(main.getName() + "-original").get();
    assertThat(mainOrig.getHash()).isEqualTo(main.getHash());
    Reference mainCompleted = api.getReference().refName(main.getName() + "-completed").get();
    assertThat(mainCompleted.getHash()).isEqualTo(main.getHash());

    for (int i = 1; i < numTables; i++) {
      assertThat(contentWithoutId(main.getName(), ContentKey.of("test-" + i))).isEqualTo(table1);
    }
  }

  @Test
  void retries() throws NessieNotFoundException, NessieConflictException {
    Branch root = api.getDefaultBranch();
    // This branch will have concurrent changes
    AtomicReference<Branch> main = new AtomicReference<>(root);
    main.set(create(api, main.get(), table1, key1));

    // This branch will be stable and will migrate in the first attempt
    Branch test =
        (Branch) api.createReference().reference(Branch.of("test1", root.getHash())).create();
    test = create(api, test, table1, key1);

    AtomicInteger count = new AtomicInteger();
    Consumer<String> stdoutWatcher =
        text -> {
          if (text.contains("Reassigning references")) {
            // Simulate some concurrent changes
            try {
              int c = count.getAndIncrement();
              if (c == 0) {
                main.set(create(api, api.getDefaultBranch(), table2, key2));
              } else if (c == 1) {
                main.set(create(api, api.getDefaultBranch(), table3, key3));
              }
            } catch (Exception e) {
              throw new RuntimeException(e);
            }
          }
        };

    ProcessResult result = runMain(stdoutWatcher, "--all");

    assertThat(count.get()).isEqualTo(3); // 2 failed attempts + 1 successful

    Reference mainOrig = api.getReference().refName(main.get().getName() + "-original").get();
    assertThat(mainOrig.getHash()).isEqualTo(main.get().getHash());
    Reference mainCompleted = api.getReference().refName(main.get().getName() + "-completed").get();
    assertThat(mainCompleted.getHash()).isEqualTo(main.get().getHash());

    assertThat(contentWithoutId(api.getDefaultBranch().getName(), key1)).isEqualTo(table1);
    assertThat(contentWithoutId(api.getDefaultBranch().getName(), key2)).isEqualTo(table2);
    assertThat(contentWithoutId(api.getDefaultBranch().getName(), key3)).isEqualTo(table3);

    assertThat(api.getDefaultBranch().getHash()).isNotEqualTo(main.get().getHash());

    Reference testOrig = api.getReference().refName(test.getName() + "-original").get();
    assertThat(testOrig.getHash()).isEqualTo(test.getHash());
    Reference testCompleted = api.getReference().refName(test.getName() + "-completed").get();
    assertThat(testCompleted.getHash()).isEqualTo(test.getHash());

    assertThat(api.getReference().refName(test.getName()).get().getHash())
        .isNotEqualTo(test.getHash());

    // Temporary references should not be migrated.
    assertThat(result.getStdOutLines())
        .noneSatisfy(l -> assertThat(l).matches("Completed migration.*-tmp-.*"));
  }
}

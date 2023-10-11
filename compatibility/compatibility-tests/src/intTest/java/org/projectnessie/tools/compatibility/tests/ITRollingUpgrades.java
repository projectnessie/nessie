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
package org.projectnessie.tools.compatibility.tests;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.projectnessie.client.api.NessieApiV1;
import org.projectnessie.model.Branch;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.Operation.Put;
import org.projectnessie.model.Reference;
import org.projectnessie.tools.compatibility.api.NessieAPI;
import org.projectnessie.tools.compatibility.api.NessieVersion;
import org.projectnessie.tools.compatibility.api.TargetVersion;
import org.projectnessie.tools.compatibility.api.Version;
import org.projectnessie.tools.compatibility.internal.RollingUpgradesExtension;

@ExtendWith(RollingUpgradesExtension.class)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@Tag("nessie-multi-env")
public class ITRollingUpgrades {

  public static final String NO_ANCESTOR =
      "2e1cfa82b035c26cbbbdae632cea070514eb8b773f616aaeaf668e2f0be8f10d";

  /** Nessie source version of the rolling upgrade. */
  @NessieVersion Version version;

  /** API of the "old" Nessie version instance. */
  @NessieAPI(targetVersion = TargetVersion.TESTED)
  NessieApiV1 api;

  /** API of the "current" (in-tree) Nessie version instance. */
  @NessieAPI(targetVersion = TargetVersion.CURRENT)
  NessieApiV1 apiCurrent;

  static List<Reference> createdReferences = new ArrayList<>();
  static Map<ContentKey, IcebergTable> createdContent = new LinkedHashMap<>();

  @BeforeAll
  static void cleared() {
    // The Nessie repository is reinitialized for every rolling-upgrade version combination.
    createdReferences.clear();
    createdContent.clear();
  }

  @Test
  @Order(100)
  void defaultBranchInOld() throws Exception {
    Branch defaultBranch = api.getDefaultBranch();
    assertThat(defaultBranch).extracting(Branch::getName).isEqualTo("main");
  }

  @Test
  @Order(101)
  void defaultBranchInNew() throws Exception {
    Branch defaultBranch = apiCurrent.getDefaultBranch();
    assertThat(defaultBranch).extracting(Branch::getName).isEqualTo("main");
  }

  @Test
  @Order(102)
  void createBranchInOld() throws Exception {
    Branch branch = Branch.of("branch-" + version, NO_ANCESTOR);
    Reference created = api.createReference().sourceRefName("main").reference(branch).create();
    createdReferences.add(created);

    assertThat(created).isEqualTo(branch);
  }

  @Test
  @Order(103)
  void createBranchInNew() throws Exception {
    Branch branch = Branch.of("branchCurrent-" + version, NO_ANCESTOR);
    Reference created =
        apiCurrent.createReference().sourceRefName("main").reference(branch).create();
    createdReferences.add(created);

    assertThat(created).isEqualTo(branch);
  }

  static List<Reference> checkCreatedReferences() {
    return createdReferences;
  }

  @ParameterizedTest
  @MethodSource("checkCreatedReferences")
  @Order(105)
  void checkCreatedReferencesInOld(Reference reference) throws Exception {
    Reference ref = api.getReference().refName(reference.getName()).get();
    assertThat(ref).isEqualTo(reference);
  }

  @ParameterizedTest
  @MethodSource("checkCreatedReferences")
  @Order(105)
  void checkCreatedReferencesInNew(Reference reference) throws Exception {
    Reference ref = apiCurrent.getReference().refName(reference.getName()).get();
    assertThat(ref).isEqualTo(reference);
  }

  @Test
  @Order(106)
  void commitDataInOld() throws Exception {
    ThreadLocalRandom tlr = ThreadLocalRandom.current();

    ContentKey key = ContentKey.of("key-v" + version.toString());
    IcebergTable table =
        IcebergTable.of(
            "metadata-" + version,
            tlr.nextLong(),
            tlr.nextInt(1234),
            tlr.nextInt(1234),
            tlr.nextInt(1234));

    Branch branch = api.getDefaultBranch();

    api.commitMultipleOperations()
        .branch(branch)
        .operation(Put.of(key, table))
        .commitMeta(CommitMeta.fromMessage("Commit old " + version))
        .commit();

    createdContent.put(key, table);
  }

  @Test
  @Order(107)
  void commitDataInNew() throws Exception {
    ThreadLocalRandom tlr = ThreadLocalRandom.current();

    Branch branch = apiCurrent.getDefaultBranch();

    ContentKey key = ContentKey.of("keyCurrent-v" + version.toString());
    IcebergTable table =
        IcebergTable.of(
            "metadataCurrent-" + version,
            tlr.nextLong(),
            tlr.nextInt(1234),
            tlr.nextInt(1234),
            tlr.nextInt(1234));

    apiCurrent
        .commitMultipleOperations()
        .branch(branch)
        .operation(Put.of(key, table))
        .commitMeta(CommitMeta.fromMessage("Commit new " + version))
        .commit();

    createdContent.put(key, table);
  }

  static Stream<Arguments> verifyContent() {
    return createdContent.entrySet().stream().map(e -> arguments(e.getKey(), e.getValue()));
  }

  @ParameterizedTest
  @MethodSource("verifyContent")
  @Order(109)
  void verifyContentInOld(ContentKey key, IcebergTable table) throws Exception {
    Map<ContentKey, Content> contents = api.getContent().key(key).refName("main").get();

    assertThat(contents)
        .extractingByKey(key)
        .extracting(AbstractCompatibilityTests::withoutContentId)
        .isEqualTo(table);
  }

  @ParameterizedTest
  @MethodSource("verifyContent")
  @Order(110)
  void verifyContentInNew(ContentKey key, IcebergTable table) throws Exception {
    Map<ContentKey, Content> contents = apiCurrent.getContent().key(key).refName("main").get();

    assertThat(contents)
        .extractingByKey(key)
        .extracting(AbstractCompatibilityTests::withoutContentId)
        .isEqualTo(table);
  }
}

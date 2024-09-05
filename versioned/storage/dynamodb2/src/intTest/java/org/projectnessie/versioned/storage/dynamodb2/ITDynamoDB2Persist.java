/*
 * Copyright (C) 2024 Dremio
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
package org.projectnessie.versioned.storage.dynamodb2;

import static java.util.Objects.requireNonNull;
import static org.projectnessie.versioned.storage.common.logic.Logics.repositoryLogic;
import static org.projectnessie.versioned.storage.common.objtypes.ContentValueObj.contentValue;

import java.util.List;
import java.util.Optional;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.projectnessie.nessie.relocated.protobuf.ByteString;
import org.projectnessie.versioned.storage.common.config.StoreConfig;
import org.projectnessie.versioned.storage.common.exceptions.ObjTooLargeException;
import org.projectnessie.versioned.storage.common.logic.RepositoryLogic;
import org.projectnessie.versioned.storage.common.persist.Obj;
import org.projectnessie.versioned.storage.common.persist.ObjId;
import org.projectnessie.versioned.storage.common.persist.Persist;
import org.projectnessie.versioned.storage.commontests.AbstractPersistTests;
import org.projectnessie.versioned.storage.dynamodbtests2.DynamoDB2BackendTestFactory;
import org.projectnessie.versioned.storage.testextension.BackendTestFactory;
import org.projectnessie.versioned.storage.testextension.NessieBackend;
import org.projectnessie.versioned.storage.testextension.NessiePersist;
import org.projectnessie.versioned.storage.testextension.PersistExtension;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

@NessieBackend(DynamoDB2BackendTestFactory.class)
public class ITDynamoDB2Persist extends AbstractPersistTests {

  @Nested
  @ExtendWith({PersistExtension.class, SoftAssertionsExtension.class})
  public class DynamoDbHardItemSizeLimits {
    @InjectSoftAssertions protected SoftAssertions soft;

    @NessiePersist protected Persist persist;

    @Test
    public void dynamoDbHardItemSizeLimit() throws Exception {
      persist.storeObj(
          contentValue(
              ObjId.randomObjId(), 4200L, "foo", 42, ByteString.copyFrom(new byte[350 * 1024])));

      persist.storeObjs(
          new Obj[] {
            contentValue(
                ObjId.randomObjId(), 4200L, "foo", 42, ByteString.copyFrom(new byte[350 * 1024]))
          });

      // DynamoDB's hard 400k limit
      soft.assertThatThrownBy(
              () ->
                  persist.storeObj(
                      contentValue(
                          ObjId.randomObjId(),
                          4200L,
                          "foo",
                          42,
                          ByteString.copyFrom(new byte[400 * 1024]))))
          .isInstanceOf(ObjTooLargeException.class);
      soft.assertThatThrownBy(
              () ->
                  persist.storeObjs(
                      new Obj[] {
                        contentValue(
                            ObjId.randomObjId(),
                            4200L,
                            "foo",
                            42,
                            ByteString.copyFrom(new byte[400 * 1024]))
                      }))
          .isInstanceOf(ObjTooLargeException.class);
    }
  }

  @Nested
  @ExtendWith({PersistExtension.class, SoftAssertionsExtension.class})
  public class TablePrefixes {
    @InjectSoftAssertions protected SoftAssertions soft;

    @NessiePersist(initializeRepo = false)
    protected BackendTestFactory factory;

    @Test
    void tablePrefix() {
      DynamoDB2BackendTestFactory dynamoDBBackendTestFactory =
          (DynamoDB2BackendTestFactory) factory;
      try (DynamoDB2Backend backendA =
              dynamoDBBackendTestFactory.createNewBackend(
                  dynamoDBBackendTestFactory
                      .dynamoDBConfigBuilder()
                      .tablePrefix(Optional.of("instanceA"))
                      .build(),
                  true);
          DynamoDB2Backend backendB =
              dynamoDBBackendTestFactory.createNewBackend(
                  dynamoDBBackendTestFactory
                      .dynamoDBConfigBuilder()
                      .tablePrefix(Optional.of("instanceB"))
                      .build(),
                  true)) {

        DynamoDbClient clientA = requireNonNull(backendA.client());
        DynamoDbClient clientB = requireNonNull(backendB.client());

        List<String> expectedTables = List.of();
        soft.assertThat(clientA.listTables().tableNames())
            .containsExactlyInAnyOrderElementsOf(expectedTables);
        soft.assertThat(clientB.listTables().tableNames())
            .containsExactlyInAnyOrderElementsOf(expectedTables);

        // Setup "A"

        backendA.setupSchema();

        Persist persistA = backendA.createFactory().newPersist(StoreConfig.Adjustable.empty());
        RepositoryLogic repoA = repositoryLogic(persistA);

        expectedTables = List.of("instanceA_refs", "instanceA_objs");

        soft.assertThat(clientA.listTables().tableNames())
            .containsExactlyInAnyOrderElementsOf(expectedTables);
        soft.assertThat(clientB.listTables().tableNames())
            .containsExactlyInAnyOrderElementsOf(expectedTables);

        soft.assertThat(repoA.repositoryExists()).isFalse();
        repoA.initialize("main");
        soft.assertThat(repoA.repositoryExists()).isTrue();

        // Setup "B"

        backendB.setupSchema();

        Persist persistB = backendB.createFactory().newPersist(StoreConfig.Adjustable.empty());
        RepositoryLogic repoB = repositoryLogic(persistB);

        expectedTables =
            List.of("instanceA_refs", "instanceA_objs", "instanceB_refs", "instanceB_objs");

        soft.assertThat(clientA.listTables().tableNames())
            .containsExactlyInAnyOrderElementsOf(expectedTables);
        soft.assertThat(clientB.listTables().tableNames())
            .containsExactlyInAnyOrderElementsOf(expectedTables);

        soft.assertThat(repoB.repositoryExists()).isFalse();
        repoB.initialize("main");
        soft.assertThat(repoB.repositoryExists()).isTrue();
      }
    }
  }
}

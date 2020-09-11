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

package com.dremio.nessie.backend.dynamodb;

import java.io.IOException;

import org.eclipse.microprofile.metrics.MetricRegistry.Type;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import com.dremio.nessie.backend.Backend;
import com.dremio.nessie.model.BranchControllerObject;
import com.dremio.nessie.model.BranchControllerReference;
import com.dremio.nessie.model.ImmutableBranchControllerObject;
import com.dremio.nessie.model.ImmutableBranchControllerReference;
import com.dremio.nessie.model.VersionedWrapper;

import io.smallrye.metrics.MetricRegistries;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughput;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;

/**
 * Dynamo db serializer test.
 */
@SuppressWarnings("AbbreviationAsWordInName")
@ExtendWith(LocalDynamoDB.class)
public class ITTestDynamo {

  private Backend backend;

  @SuppressWarnings("MissingJavadocMethod")
  @BeforeAll
  public static void start(DynamoDbClient client) throws Exception {
    for (String t: new String[]{"NessieGitObjectDatabase", "NessieGitRefDatabase"}) {
      CreateTableRequest request =
          CreateTableRequest.builder()
                            .attributeDefinitions(AttributeDefinition.builder()
                                                                     .attributeName("uuid")
                                                                     .attributeType(
                                                                       ScalarAttributeType.S)
                                                                     .build())
                            .keySchema(KeySchemaElement.builder()
                                                       .attributeName("uuid")
                                                       .keyType(KeyType.HASH)
                                                       .build())
                            .provisionedThroughput(ProvisionedThroughput.builder()
                                                                        .readCapacityUnits(10L)
                                                                        .writeCapacityUnits(10L)
                                                                        .build())
                            .tableName(t)
                            .build();
      client.createTable(request);
    }
  }

  @BeforeEach
  public void client(DynamoDbClient client) {
    backend = new DynamoDbBackend(client, MetricRegistries.get(Type.APPLICATION));
  }

  @AfterEach
  public void close() {
    ((DynamoDbBackend) backend).close();
    backend = null;
  }

  @Test
  public void testCrud() {
    BranchControllerObject table = ImmutableBranchControllerObject.builder()
                                                                  .id("1")
                                                                  .data(new byte[]{1})
                                                                  .updateTime(0L)
                                                                  .type(0)
                                                                  .build();
    backend.gitBackend().update("1", new VersionedWrapper<>(table));

    Assertions.assertEquals(1, backend.gitBackend().getAll(true).size());

    VersionedWrapper<BranchControllerObject> versionedGitObject = backend.gitBackend().get("1");
    table = versionedGitObject.getObj();
    Assertions.assertEquals((byte) 1, table.getData()[0]);

    table = ImmutableBranchControllerObject.builder().from(table).data(new byte[]{1, 2}).build();
    backend.gitBackend().update("1", versionedGitObject.update(table));

    versionedGitObject = backend.gitBackend().get("1");
    table = versionedGitObject.getObj();
    Assertions.assertEquals((byte) 1, table.getData()[0]);
    Assertions.assertEquals((byte) 2, table.getData()[1]);

    backend.gitBackend().remove("1");

    Assertions.assertNull(backend.gitBackend().get("1"));
    Assertions.assertTrue(backend.gitBackend().getAll(true).isEmpty());
  }

  @Test
  public void testOptimisticLocking() throws IOException {
    BranchControllerReference table = ImmutableBranchControllerReference.builder()
                                                                        .id("1")
                                                                        .updateTime(0L)
                                                                        .refId("2")
                                                                        .build();
    backend.gitRefBackend().update("1", new VersionedWrapper<>(table));

    VersionedWrapper<BranchControllerReference> versionGitRef1 = backend.gitRefBackend().get("1");
    VersionedWrapper<BranchControllerReference> versionGitRef2 = backend.gitRefBackend().get("1");

    BranchControllerReference table1 = ImmutableBranchControllerReference.builder()
                                                                         .from(
                                                                           versionGitRef1.getObj())
                                                                         .refId("x")
                                                                         .build();
    backend.gitRefBackend().update("1", versionGitRef1.update(table1));

    BranchControllerReference table2 = ImmutableBranchControllerReference.builder()
                                                                         .from(
                                                                           versionGitRef2.getObj())
                                                                         .refId("xx")
                                                                         .build();
    try {
      backend.gitRefBackend().update("", versionGitRef2.update(table2));
      Assertions.fail();
    } catch (Throwable t) {
      Assertions.assertTrue(t instanceof ConditionalCheckFailedException);
    }
    versionGitRef2 = backend.gitRefBackend().get("1");
    table2 = ImmutableBranchControllerReference.builder()
                                               .from(versionGitRef2.getObj())
                                               .refId("xx")
                                               .build();
    backend.gitRefBackend().update("1", versionGitRef2.update(table2));

    VersionedWrapper<BranchControllerReference> versionedGitRef = backend.gitRefBackend()
                                                                         .get("1");
    table = versionedGitRef.getObj();
    Assertions.assertEquals("xx", table.getRefId());

    backend.gitBackend().remove("1");
    Assertions.assertNull(backend.gitBackend().get("1"));
  }
}

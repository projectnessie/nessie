package com.dremio.nessie.versioned.impl.condition;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;

import com.dremio.nessie.backend.dynamodb.LocalDynamoDB;
import com.google.common.collect.ImmutableMap;

import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbTable;
import software.amazon.awssdk.enhanced.dynamodb.TableSchema;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;

@ExtendWith(LocalDynamoDB.class)
public class ITTestProjectionCondition {

  private void insertToy(DynamoDbClient client, TestInfo info) {
    final String tableName = info.getTestMethod().get().getName();
    DynamoDbTable<Toy> table = DynamoDbEnhancedClient.builder().dynamoDbClient(client).build()
        .table(tableName, TableSchema.fromBean(Toy.class));
    table.createTable();
    Toy t = new Toy();
    t.setStr("foo");
    t.setAbort1("myabort");
    t.setId("1");
    Toy.Bauble bauble1 = new Toy.Bauble();
    bauble1.setId("a");
    Toy.Bauble bauble2 = new Toy.Bauble();
    bauble2.setId("b");
    Toy.Bauble bauble3 = new Toy.Bauble();
    bauble3.setId("c");
    Toy.Bauble bauble4 = new Toy.Bauble();
    bauble4.setId("d");

    t.setBaubles(Arrays.asList(bauble1, bauble2, bauble3, bauble4));
    table.putItem(t);
  }

  @Test
  void projectFewFields(DynamoDbClient client, TestInfo info) {
    insertToy(client, info);
    final String tableName = info.getTestMethod().get().getName();
    Map<String, AttributeValue> values = client.getItem(GetItemRequest.builder()
            .tableName(tableName)
            .projectionExpression(ProjectionExpression.builder()
                .addPaths(ExpressionPath.builder("str").build())
                .addPaths(ExpressionPath.builder("abort1").build())
                .build().toProjectionExpression())
            .key(ImmutableMap.<String, AttributeValue>of("id", AttributeValue.builder().s("1").build()))
            .build()
            ).item();
    assertEquals(AttributeValue.builder().s("foo").build(), values.get("str"));
    assertEquals(null, values.get("baubles"));
  }

  @Test
  void projectList(DynamoDbClient client, TestInfo info) {
    insertToy(client, info);
    final String tableName = info.getTestMethod().get().getName();
    Map<String, AttributeValue> values = client.getItem(GetItemRequest.builder()
            .tableName(tableName)
            .projectionExpression(ProjectionExpression.builder()
                .addPaths(ExpressionPath.builder("baubles").build())
                .build().toProjectionExpression())
            .key(ImmutableMap.<String, AttributeValue>of("id", AttributeValue.builder().s("1").build()))
            .build()
            ).item();
    Toy toy = TableSchema.fromBean(Toy.class).mapToItem(values);
    assertNotEquals(null, toy.getBaubles());
    assertEquals(4, toy.getBaubles().size());
    assertEquals("a", toy.getBaubles().get(0).getId());
    assertEquals("b", toy.getBaubles().get(1).getId());
    assertEquals("c", toy.getBaubles().get(2).getId());
    assertEquals("d", toy.getBaubles().get(3).getId());
  }


  @Test
  void projectSecondListItem(DynamoDbClient client, TestInfo info) {
    insertToy(client, info);
    final String tableName = info.getTestMethod().get().getName();
    Map<String, AttributeValue> values = client.getItem(GetItemRequest.builder()
            .tableName(tableName)
            .projectionExpression(ProjectionExpression.builder()
                .addPaths(ExpressionPath.builder("str").build())
                .addPaths(ExpressionPath.builder("baubles").position(1).build())
                .addPaths(ExpressionPath.builder("baubles").position(3).build())
                .build().toProjectionExpression())
            .key(ImmutableMap.<String, AttributeValue>of("id", AttributeValue.builder().s("1").build()))
            .build()
            ).item();
    System.out.println(ProjectionExpression.builder()
        .addPaths(ExpressionPath.builder("str").build())
        .addPaths(ExpressionPath.builder("baubles").position(1).build())
        .addPaths(ExpressionPath.builder("baubles").position(3).build())
        .build().toString());
    Toy toy = TableSchema.fromBean(Toy.class).mapToItem(values);
    assertTrue(toy.getBaubles().size() == 2);

    assertEquals("b", toy.getBaubles().get(0).getId());
    assertEquals("d", toy.getBaubles().get(1).getId());
  }


}

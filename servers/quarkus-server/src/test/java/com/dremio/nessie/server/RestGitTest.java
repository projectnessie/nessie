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

package com.dremio.nessie.server;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.dremio.nessie.model.Branch;
import com.dremio.nessie.model.CommitMeta;
import com.dremio.nessie.model.Contents;
import com.dremio.nessie.model.IcebergTable;
import com.dremio.nessie.model.ImmutableBranch;
import com.dremio.nessie.model.ImmutableIcebergTable;
import com.dremio.nessie.model.ImmutableMultiContents;
import com.dremio.nessie.model.ImmutablePut;
import com.dremio.nessie.model.ImmutablePutContents;
import com.dremio.nessie.model.ImmutableTag;
import com.dremio.nessie.model.MultiContents;
import com.dremio.nessie.model.NessieObjectKey;
import com.dremio.nessie.model.Operation.Put;
import com.dremio.nessie.model.PutContents;
import com.dremio.nessie.model.Reference;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.security.TestSecurity;
import io.restassured.RestAssured;
import io.restassured.common.mapper.TypeRef;
import io.restassured.http.ContentType;
import io.restassured.response.Response;
import io.restassured.specification.RequestSpecification;

@QuarkusTest
public class RestGitTest {

  @BeforeEach
  public void enableLogging() {
    RestAssured.enableLoggingOfRequestAndResponseIfValidationFails();
  }

  @Test
  @TestSecurity(authorizationEnabled = false)
  public void testBasic() {
    Branch mainx = ImmutableBranch.builder().name("mainx").build();

    given().when().get("/api/v1/tree/mainx").then().statusCode(404);
    given()
      .when().body(mainx).contentType(ContentType.JSON).post("/api/v1/tree")
      .then().statusCode(204);

    Reference[] references = given().when().get("/api/v1/trees").then().statusCode(200).extract().as(Reference[].class);
    Assertions.assertEquals(1, references.length);
    Assertions.assertEquals("mainx", references[0].getName());

    Reference reference = given().when().get("/api/v1/tree/mainx").then()
                 .statusCode(200)
                 .extract()
                 .as(Reference.class);
    assertEquals("mainx", reference.getName());

    Branch newReference = ImmutableBranch.builder()
        .hash(reference.getHash())
        .name("test")
        .build();
    given().when().body(newReference).contentType(ContentType.JSON).post("/api/v1/tree").then().statusCode(204);
    assertEquals(newReference, given().when().get("/api/v1/tree/test").then()
           .statusCode(200).extract().as(Branch.class));

    IcebergTable table = ImmutableIcebergTable.builder()
                                .metadataLocation("/the/directory/over/there")
                                .build();
    PutContents put = ImmutablePutContents.builder().contents(table).branch(newReference).build();

    given().when().body(put).contentType(ContentType.JSON).post("/api/v1/contents/xxx.test").then().statusCode(204);

    Put[] updates = new Put[11];
    for (int i = 0; i < 10; i++) {
      updates[i] =
          ImmutablePut.builder().key(new NessieObjectKey(Arrays.asList("item", Integer.toString(i))))
          .object(ImmutableIcebergTable.builder()
                                 .metadataLocation("/the/directory/over/there/" + i)
                                 .build())
          .build();
    }
    updates[10] = ImmutablePut.builder().key(new NessieObjectKey(Arrays.asList("xxx","test")))
        .object(ImmutableIcebergTable.builder().metadataLocation("/the/directory/over/there/has/been/moved").build())
        .build();

    Reference branch = given().when().get("/api/v1/tree/test").as(Reference.class);
    MultiContents contents = ImmutableMultiContents.builder()
        .addOperations(updates)
        .branch((Branch) branch)
        .build();

    rest().body(contents).put("/api/v1/contents/multi").then().statusCode(204);

    Response res = given().when().queryParam("ref", "test").get("/api/v1/contents/xxx.test").then().extract().response();
    Assertions.assertEquals(updates[10].getObject(), res.body().as(Contents.class));

    table = ImmutableIcebergTable.builder()
        .metadataLocation("/the/directory/over/there/has/been/moved/again")
        .build();

    Branch b2 = rest().get("/api/v1/tree/test").as(Branch.class);
    rest().body(ImmutablePutContents.builder().branch(b2).contents(table).build()).contentType(ContentType.JSON)
           .post("/api/v1/contents/xxx.test").then().statusCode(204);
    Contents returned = rest().queryParam("ref", "test").get("/api/v1/contents/xxx.test").then().statusCode(200).extract().as(Contents.class);
    Assertions.assertEquals(table, returned);

    Branch b3 = rest().get("/api/v1/tree/test").as(Branch.class);
    rest().body(
        ImmutableTag.builder()
          .name("tagtest")
          .hash(b3.getHash())
          .build())
    .post("/api/v1/tree/").then().statusCode(204);

    rest().get("/api/v1/tree/tagtest").then().statusCode(200).body("hash", equalTo(b3.getHash()));

    rest().body(ImmutableTag.builder().name("tagtest").hash("aa").build()).delete("/api/v1/tree").then().statusCode(412);

    rest().body(ImmutableTag.builder().name("tagtest").hash(b3.getHash())).delete("/api/v1/tree").then().statusCode(204);
//
//
//    res = given().when().get("/api/v1/objects/test/log").then().statusCode(200).extract().response();
//    Map<String, CommitMeta> logs = res.body().as(new TypeRef<Map<String, CommitMeta>>() {});
//    Assertions.assertEquals(4, logs.size());
//    Assertions.assertEquals(13, logs.values().stream().mapToInt(CommitMeta::changes).sum());
//
//    etag = given().when().get("/api/v1/objects/test").getHeader("ETag");
//    given().when().header("If-Match",etag).delete("/api/v1/objects/test").then().statusCode(200);
//    etag = given().when().get("/api/v1/objects/mainx").getHeader("ETag");
//    given().when().header("If-Match",etag).delete("/api/v1/objects/mainx").then().statusCode(200);
  }

  private static RequestSpecification rest() {
    return given().when().contentType(ContentType.JSON);
  }
  @Test
  @TestSecurity(authorizationEnabled = false)
  public void testNewBranch() {
    ReferenceWithType<Branch> mainx = ReferenceWithType.of(ImmutableBranch.builder()
                                                                         .id(null)
                                                                         .name("mainx")
                                                                         .build());
    int statusCode = given().when().get("/api/v1/objects/mainx").getStatusCode();
    if (statusCode == 404) {
      given()
        .when().body(mainx).contentType(ContentType.JSON).post("/api/v1/objects/mainx")
        .then().statusCode(201);
    }

    Table[] updates = new Table[10];
    for (int i = 0; i < 10; i++) {
      updates[i] = ImmutableTable.builder()
                                 .id("xxx.test" + i)
                                 .name("test" + i)
                                 .namespace("xxx")
                                 .metadataLocation("/the/directory/over/there/" + i)
                                 .build();
    }
    String etag = given().when().get("/api/v1/objects/mainx").getHeader("ETag");
    given().when().header("If-Match", etag).body(updates).contentType(ContentType.JSON)
           .put("/api/v1/objects/mainx/tables").then().statusCode(200);
    etag = given().when().get("/api/v1/objects/mainx").getHeader("ETag");

    ReferenceWithType<Branch> test = ReferenceWithType.of(ImmutableBranch.builder()
                                 .id(etag.replace("\"", ""))
                                 .name("test2")
                                 .build());
    given()
      .when().header("If-Match", etag).body(test)
      .contentType(ContentType.JSON).post("/api/v1/objects/test2").then().statusCode(201);

    for (int i = 0; i < 10; i++) {
      Table bt = updates[i];
      Response res = given().when().get("/api/v1/objects/test2/tables/" + bt.getId())
                            .then().statusCode(200).extract().response();
      Assertions.assertEquals(bt, res.body().as(Table.class));
    }

    etag = given().when().get("/api/v1/objects/test2").getHeader("ETag");
    given().when().header("If-Match", etag)
           .delete("/api/v1/objects/test2/tables/" + updates[0].getId()).then().statusCode(200);

    given().when().get("/api/v1/objects/test2/" + updates[0].getId()).then().statusCode(404);

    etag = given().when().get("/api/v1/objects/test2").getHeader("ETag");
    given().when().header("If-Match", etag).delete("/api/v1/objects/test2").then().statusCode(200);


    given().when().get("/api/v1/objects/test2").then().statusCode(404);
  }

  @Test
  @TestSecurity(authorizationEnabled = false)
  public void testOptimisticLocking() {
    ReferenceWithType<Branch> test = ReferenceWithType.of(ImmutableBranch.builder()
                                 .id(null)
                                 .name("test3")
                                 .build());
    given()
      .when().body(test).contentType(ContentType.JSON).post("/api/v1/objects/test3")
      .then().statusCode(201);

    Table table = ImmutableTable.builder()
                                .id("xxx.test")
                                .name("test")
                                .namespace("xxx")
                                .metadataLocation("/the/directory/over/there")
                                .build();
    String etag = given().when().get("/api/v1/objects/test3").getHeader("ETag");
    given().when().body(table).header("If-Match", etag).contentType(ContentType.JSON)
           .post("/api/v1/objects/test3/tables/xxx.test").then().statusCode(201);

    String etagStart = given().when().get("/api/v1/objects/test3").getHeader("ETag");
    table = ImmutableTable.builder()
                          .id("xxx.test")
                          .name("test")
                          .namespace("xxx")
                          .metadataLocation("/the/directory/over/there/has/been/moved")
                          .build();
    given().when().body(table).header("If-Match", etagStart).contentType(ContentType.JSON)
           .put("/api/v1/objects/test3/tables/xxx.test").then().statusCode(200);

    table = ImmutableTable.builder()
                          .id("xxx.test")
                          .name("test")
                          .namespace("xxx")
                          .metadataLocation("/the/directory/over/there/has/been/moved/again")
                          .build();
    given().when().body(table).header("If-Match", etagStart).contentType(ContentType.JSON)
           .put("/api/v1/objects/test3/tables/xxx.test").then().statusCode(412);

    String etagNew = given().when().get("/api/v1/objects/test3").getHeader("ETag");
    given().when().body(table).header("If-Match", etagNew).contentType(ContentType.JSON)
           .put("/api/v1/objects/test3/tables/xxx.test").then().statusCode(200);
  }

}

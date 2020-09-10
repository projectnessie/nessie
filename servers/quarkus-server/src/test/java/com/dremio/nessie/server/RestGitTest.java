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

import java.util.Map;

import org.jose4j.base64url.Base64;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.dremio.nessie.model.Branch;
import com.dremio.nessie.model.CommitMeta;
import com.dremio.nessie.model.ImmutableBranch;
import com.dremio.nessie.model.ImmutableTable;
import com.dremio.nessie.model.ImmutableTag;
import com.dremio.nessie.model.ReferenceWithType;
import com.dremio.nessie.model.Table;

import io.quarkus.test.junit.QuarkusTest;
import io.restassured.common.mapper.TypeRef;
import io.restassured.http.ContentType;
import io.restassured.http.Header;
import io.restassured.response.Response;

@QuarkusTest
public class RestGitTest {
  private static final String token;
  private static final Header header;

  static {
    token = "Basic " + Base64.encode("admin_user:test123".getBytes());
    header = new Header("Authorization", token);
  }
  
  protected Header header() {
    return header;
  }

  @Test
  public void testBasic() {
    ReferenceWithType<Branch> main = ReferenceWithType.of(ImmutableBranch.builder()
                                                                         .id(null)
                                                                         .name("main")
                                                                         .build());
    int statusCode = given().when().header(header()).get("/api/v1/objects/main").getStatusCode();
    if (statusCode == 404) {
      given()
        .when().header(header()).body(main).contentType(ContentType.JSON).post("/api/v1/objects/main")
        .then().statusCode(201);
    }

    Response res = given().when().header(header()).get("/api/v1/objects").then().statusCode(200).extract().response();
    ReferenceWithType<Branch>[] branches = res.body().as(ReferenceWithType[].class);
    Assertions.assertEquals(1, branches.length);
    Assertions.assertEquals("main", branches[0].getReference().getName());

    res = given().when().header(header()).get("/api/v1/objects/main").then()
                 .statusCode(200).body("reference.name", equalTo(main.getReference().getName())).extract().response();
    main = res.body().as(new TypeRef<ReferenceWithType<Branch>>() {
    });
    ReferenceWithType<Branch> test = ReferenceWithType.of(ImmutableBranch.builder()
                                                                         .id(main.getReference().getId())
                                                                         .name("test")
                                                                         .build());
    given().when().header(header()).body(test).contentType(ContentType.JSON).post("/api/v1/objects/test").then().statusCode(201);
    given().when().header(header()).get("/api/v1/objects/test").then()
           .statusCode(200).body("reference.name", equalTo(test.getReference().getName()))
           .body("reference.id", equalTo(main.getReference().getId()));

    Table table = ImmutableTable.builder()
                                .id("xxx.test")
                                .name("test")
                                .namespace("xxx")
                                .metadataLocation("/the/directory/over/there")
                                .build();

    String etag = given().when().header(header()).get("/api/v1/objects/test").getHeader("ETag");
    given().when().header(header()).body(table).header("If-Match", etag).contentType(ContentType.JSON)
           .post("/api/v1/objects/test/tables/xxx.test").then().statusCode(201);

    Table[] updates = new Table[11];
    for (int i = 0; i < 10; i++) {
      updates[i] = ImmutableTable.builder()
                                 .id("xxx.test" + i)
                                 .name("test" + i)
                                 .namespace("xxx")
                                 .metadataLocation("/the/directory/over/there/" + i)
                                 .build();
    }
    updates[10] = ImmutableTable.builder()
                                .id("xxx.test")
                                .name("test")
                                .namespace("xxx")
                                .metadataLocation(
                                  "/the/directory/over/there/has/been/moved")
                                .build();

    etag = given().when().header(header()).get("/api/v1/objects/test").getHeader("ETag");
    given().when().header(header()).header("If-Match", etag).body(updates).contentType(ContentType.JSON)
           .put("/api/v1/objects/test/tables").then().statusCode(200);

    table = ImmutableTable.builder()
                          .id("xxx.test")
                          .name("test")
                          .namespace("xxx")
                          .metadataLocation(
                            "/the/directory/over/there/has/been/moved/again")
                          .build();
    res = given().when().header(header()).get("/api/v1/objects/test/tables/xxx.test").then().extract().response();
    Assertions.assertEquals(updates[10], res.body().as(Table.class));

    etag = given().when().header(header()).get("/api/v1/objects/test").getHeader("ETag");
    given().when().header(header()).header("If-Match", etag).body(table).contentType(ContentType.JSON)
           .put("/api/v1/objects/test/tables/xxx.test").then().statusCode(200);
    res = given().when().header(header()).get("/api/v1/objects/test/tables/xxx.test").then().extract().response();
    Assertions.assertEquals(table, res.body().as(Table.class));

    etag = given().when().header(header()).get("/api/v1/objects/test").getHeader("ETag");
    given().when().header(header()).contentType(ContentType.JSON).body(ReferenceWithType.of(ImmutableTag.builder()
                                                                                       .name("tagtest")
                                                                                       .id(etag.replace("\"",""))
                                                                                       .build()))
           .post("/api/v1/objects/tagtest").then().statusCode(201);

    given().when().header(header()).get("/api/v1/objects/tagtest").then().statusCode(200)
           .body("reference.id", equalTo(etag.replace("\"","")));

    given().when().header(header()).header("If-Match", etag).body(updates).contentType(ContentType.JSON)
           .put("/api/v1/objects/tagtest/tables").then().statusCode(404);


    given().when().header(header()).delete("/api/v1/objects/tagtest").then().statusCode(412);

    given().when().header(header()).header("If-Match",etag).delete("/api/v1/objects/tagtest").then().statusCode(200);


    res = given().when().header(header()).get("/api/v1/objects/test/log").then().statusCode(200).extract().response();
    Map<String, CommitMeta> logs = res.body().as(new TypeRef<Map<String, CommitMeta>>() {});
    Assertions.assertEquals(4, logs.size());
    Assertions.assertEquals(13, logs.values().stream().mapToInt(CommitMeta::changes).sum());
  }

  @Test
  public void testNewBranch() {
    ReferenceWithType<Branch> main = ReferenceWithType.of(ImmutableBranch.builder()
                                                                         .id(null)
                                                                         .name("main")
                                                                         .build());
    int statusCode = given().when().header(header()).get("/api/v1/objects/main").getStatusCode();
    if (statusCode == 404) {
      given()
        .when().header(header()).body(main).contentType(ContentType.JSON).post("/api/v1/objects/main")
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
    String etag = given().when().header(header()).get("/api/v1/objects/main").getHeader("ETag");
    given().when().header(header()).header("If-Match", etag).body(updates).contentType(ContentType.JSON)
           .put("/api/v1/objects/main/tables").then().statusCode(200);
    etag = given().when().header(header()).get("/api/v1/objects/main").getHeader("ETag");

    ReferenceWithType<Branch> test = ReferenceWithType.of(ImmutableBranch.builder()
                                 .id(etag.replace("\"", ""))
                                 .name("test2")
                                 .build());
    given()
      .when().header(header()).header("If-Match", etag).body(test)
      .contentType(ContentType.JSON).post("/api/v1/objects/test2").then().statusCode(201);

    for (int i = 0; i < 10; i++) {
      Table bt = updates[i];
      Response res = given().when().header(header()).get("/api/v1/objects/test2/tables/" + bt.getId())
                            .then().statusCode(200).extract().response();
      Assertions.assertEquals(bt, res.body().as(Table.class));
    }

    etag = given().when().header(header()).get("/api/v1/objects/test2").getHeader("ETag");
    given().when().header(header()).header("If-Match", etag)
           .delete("/api/v1/objects/test2/tables/" + updates[0].getId()).then().statusCode(200);

    given().when().header(header()).get("/api/v1/objects/test2/" + updates[0].getId()).then().statusCode(404);

    etag = given().when().header(header()).get("/api/v1/objects/test2").getHeader("ETag");
    given().when().header(header()).header("If-Match", etag).delete("/api/v1/objects/test2").then().statusCode(200);


    given().when().header(header()).get("/api/v1/objects/test2").then().statusCode(404);
  }

  @Test
  public void testOptimisticLocking() {
    ReferenceWithType<Branch> test = ReferenceWithType.of(ImmutableBranch.builder()
                                 .id(null)
                                 .name("test3")
                                 .build());
    given()
      .when().header(header()).body(test).contentType(ContentType.JSON).post("/api/v1/objects/test3")
      .then().statusCode(201);

    Table table = ImmutableTable.builder()
                                .id("xxx.test")
                                .name("test")
                                .namespace("xxx")
                                .metadataLocation("/the/directory/over/there")
                                .build();
    String etag = given().when().header(header()).get("/api/v1/objects/test3").getHeader("ETag");
    given().when().header(header()).body(table).header("If-Match", etag).contentType(ContentType.JSON)
           .post("/api/v1/objects/test3/tables/xxx.test").then().statusCode(201);

    String etagStart = given().when().header(header()).get("/api/v1/objects/test3").getHeader("ETag");
    table = ImmutableTable.builder()
                          .id("xxx.test")
                          .name("test")
                          .namespace("xxx")
                          .metadataLocation("/the/directory/over/there/has/been/moved")
                          .build();
    given().when().header(header()).body(table).header("If-Match", etagStart).contentType(ContentType.JSON)
           .put("/api/v1/objects/test3/tables/xxx.test").then().statusCode(200);

    table = ImmutableTable.builder()
                          .id("xxx.test")
                          .name("test")
                          .namespace("xxx")
                          .metadataLocation("/the/directory/over/there/has/been/moved/again")
                          .build();
    given().when().header(header()).body(table).header("If-Match", etagStart).contentType(ContentType.JSON)
           .put("/api/v1/objects/test3/tables/xxx.test").then().statusCode(412);

    String etagNew = given().when().header(header()).get("/api/v1/objects/test3").getHeader("ETag");
    given().when().header(header()).body(table).header("If-Match", etagNew).contentType(ContentType.JSON)
           .put("/api/v1/objects/test3/tables/xxx.test").then().statusCode(200);
  }

}

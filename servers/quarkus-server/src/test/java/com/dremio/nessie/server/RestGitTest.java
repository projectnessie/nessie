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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.dremio.nessie.model.Branch;
import com.dremio.nessie.model.Contents;
import com.dremio.nessie.model.IcebergTable;
import com.dremio.nessie.model.ImmutableBranch;
import com.dremio.nessie.model.ImmutableIcebergTable;
import com.dremio.nessie.model.ImmutableMultiContents;
import com.dremio.nessie.model.ImmutablePut;
import com.dremio.nessie.model.ImmutablePutContents;
import com.dremio.nessie.model.ImmutableTag;
import com.dremio.nessie.model.LogResponse;
import com.dremio.nessie.model.MultiContents;
import com.dremio.nessie.model.NessieObjectKey;
import com.dremio.nessie.model.Operation.Put;
import com.dremio.nessie.model.PutContents;
import com.dremio.nessie.model.Reference;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.security.TestSecurity;
import io.restassured.RestAssured;
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

    rest().get("/api/v1/tree/mainx").then().statusCode(404);
    rest().body(mainx).post("/api/v1/tree")
      .then().statusCode(204);

    Reference[] references = rest().get("/api/v1/trees").then().statusCode(200).extract().as(Reference[].class);
    Assertions.assertEquals(1, references.length);
    Assertions.assertEquals("mainx", references[0].getName());

    Reference reference = rest().get("/api/v1/tree/mainx").then()
                 .statusCode(200)
                 .extract()
                 .as(Reference.class);
    assertEquals("mainx", reference.getName());

    Branch newReference = ImmutableBranch.builder()
        .hash(reference.getHash())
        .name("test")
        .build();
    rest().body(newReference).post("/api/v1/tree").then().statusCode(204);
    assertEquals(newReference, rest().get("/api/v1/tree/test").then()
           .statusCode(200).extract().as(Branch.class));

    IcebergTable table = ImmutableIcebergTable.builder()
                                .metadataLocation("/the/directory/over/there")
                                .build();
    PutContents put = ImmutablePutContents.builder().contents(table).branch(newReference).build();

    rest().body(put).post("/api/v1/contents/xxx.test").then().statusCode(204);

    Put[] updates = new Put[11];
    for (int i = 0; i < 10; i++) {
      updates[i] =
          ImmutablePut.builder().key(new NessieObjectKey(Arrays.asList("item", Integer.toString(i))))
          .contents(ImmutableIcebergTable.builder()
                                 .metadataLocation("/the/directory/over/there/" + i)
                                 .build())
          .build();
    }
    updates[10] = ImmutablePut.builder().key(new NessieObjectKey(Arrays.asList("xxx","test")))
        .contents(ImmutableIcebergTable.builder().metadataLocation("/the/directory/over/there/has/been/moved").build())
        .build();

    Reference branch = rest().get("/api/v1/tree/test").as(Reference.class);
    MultiContents contents = ImmutableMultiContents.builder()
        .addOperations(updates)
        .branch((Branch) branch)
        .build();

    rest().body(contents).put("/api/v1/contents/multi").then().statusCode(204);

    Response res = rest().queryParam("ref", "test").get("/api/v1/contents/xxx.test").then().extract().response();
    Assertions.assertEquals(updates[10].getContents(), res.body().as(Contents.class));

    table = ImmutableIcebergTable.builder()
        .metadataLocation("/the/directory/over/there/has/been/moved/again")
        .build();

    Branch b2 = rest().get("/api/v1/tree/test").as(Branch.class);
    rest().body(ImmutablePutContents.builder().branch(b2).contents(table).build())
           .post("/api/v1/contents/xxx.test").then().statusCode(204);
    Contents returned = rest().queryParam("ref", "test")
        .get("/api/v1/contents/xxx.test").then().statusCode(200).extract().as(Contents.class);
    Assertions.assertEquals(table, returned);

    Branch b3 = rest().get("/api/v1/tree/test").as(Branch.class);
    rest().body(
        ImmutableTag.builder()
          .name("tagtest")
          .hash(b3.getHash())
          .build())
    .post("/api/v1/tree/").then().statusCode(204);

    rest().get("/api/v1/tree/tagtest").then().statusCode(200).body("hash", equalTo(b3.getHash()));

    rest().body(ImmutableTag.builder().name("tagtest").hash("aa").build()).delete("/api/v1/tree").then().statusCode(409);

    rest().body(ImmutableTag.builder().name("tagtest").hash(b3.getHash()).build()).delete("/api/v1/tree").then().statusCode(204);


    LogResponse log = rest().get("/api/v1/tree/test/log").then().statusCode(200).extract().as(LogResponse.class);
    Assertions.assertEquals(3, log.getOperations().size());

    rest().body(rest().get("/api/v1/tree/test").as(Branch.class)).delete("/api/v1/tree").then().statusCode(204);
    rest().body(rest().get("/api/v1/tree/mainx").as(Branch.class)).delete("/api/v1/tree").then().statusCode(204);
  }

  private static RequestSpecification rest() {
    return given().when().contentType(ContentType.JSON);
  }

  private PutContents newPut(Branch branch, String path, String metadataUrl) {
    return ImmutablePutContents.builder()
        .branch(branch)
        .contents(
            ImmutableIcebergTable.builder()
            .metadataLocation(metadataUrl)
            .build())
        .build();
  }

  private void commit(Branch b, String path, String metadataUrl) {
    rest().body(newPut(b, path, metadataUrl)).post("/api/v1/contents/xxx.test").then().statusCode(204);
  }

  private Branch getBranch(String name) {
    return rest().get("/api/v1/tree/" + name).then().statusCode(200).extract().as(Branch.class);
  }

  private Branch makeBranch(String name) {
    Branch test = ImmutableBranch.builder()
        .name(name)
        .build();
    rest().body(test).post("/api/v1/tree").then().statusCode(204);
    return test;
  }

  @Test
  @TestSecurity(authorizationEnabled = false)
  public void testOptimisticLocking() {
    makeBranch("test3");
    Branch b1 = getBranch("test3");
    commit(b1, "xxx.test", "/the/directory/over/there");

    Branch b2 = getBranch("test3");
    commit(b2, "xxx.test", "/the/directory/over/there/has/been/moved");

    Branch b3 = getBranch("test3");
    commit(b3, "xxx.test", "/the/directory/over/there/has/been/moved/again");
  }

}

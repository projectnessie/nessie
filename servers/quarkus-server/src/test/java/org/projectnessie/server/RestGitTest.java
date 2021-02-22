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

package org.projectnessie.server;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.projectnessie.model.Branch;
import org.projectnessie.model.Contents;
import org.projectnessie.model.ContentsKey;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.ImmutableBranch;
import org.projectnessie.model.ImmutableIcebergTable;
import org.projectnessie.model.ImmutableOperations;
import org.projectnessie.model.ImmutablePut;
import org.projectnessie.model.LogResponse;
import org.projectnessie.model.Operation.Put;
import org.projectnessie.model.Operations;
import org.projectnessie.model.Reference;
import org.projectnessie.model.Tag;

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
    int preSize = rest().get("trees").then().statusCode(200).extract().as(Reference[].class).length;

    rest().get("trees/tree/mainx").then().statusCode(404);
    rest().body(Branch.of("mainx", null)).post("trees/tree").then().statusCode(204);

    Reference[] references = rest().get("trees").then().statusCode(200).extract().as(Reference[].class);
    Assertions.assertEquals(preSize + 1, references.length);

    Reference reference = rest().get("trees/tree/mainx").then()
                 .statusCode(200)
                 .extract()
                 .as(Reference.class);
    assertEquals("mainx", reference.getName());

    Branch newReference = ImmutableBranch.builder()
        .hash(reference.getHash())
        .name("test")
        .build();
    rest().queryParam("expectedHash", reference.getHash()).body(Branch.of("test", null)).post("trees/tree").then().statusCode(204);
    assertEquals(newReference, rest().get("trees/tree/test").then()
           .statusCode(200).extract().as(Branch.class));

    IcebergTable table = ImmutableIcebergTable.builder()
                                .metadataLocation("/the/directory/over/there")
                                .build();

    rest()
      .body(table)
      .queryParam("branch", newReference.getName()).queryParam("hash", newReference.getHash())
      .post("contents/xxx.test")
        .then().statusCode(204);

    Put[] updates = new Put[11];
    for (int i = 0; i < 10; i++) {
      updates[i] =
          ImmutablePut.builder().key(ContentsKey.of("item", Integer.toString(i)))
          .contents(ImmutableIcebergTable.builder()
                                 .metadataLocation("/the/directory/over/there/" + i)
                                 .build())
          .build();
    }
    updates[10] = ImmutablePut.builder().key(ContentsKey.of("xxx","test"))
        .contents(ImmutableIcebergTable.builder().metadataLocation("/the/directory/over/there/has/been/moved").build())
        .build();

    Reference branch = rest().get("trees/tree/test").as(Reference.class);
    Operations contents = ImmutableOperations.builder()
        .addOperations(updates)
        .build();

    rest().body(contents).queryParam("expectedHash", branch.getHash()).post("trees/branch/{branch}/commit", branch.getName())
      .then().statusCode(204);

    Response res = rest().queryParam("ref", "test").get("contents/xxx.test").then().extract().response();
    Assertions.assertEquals(updates[10].getContents(), res.body().as(Contents.class));

    table = ImmutableIcebergTable.builder()
        .metadataLocation("/the/directory/over/there/has/been/moved/again")
        .build();

    Branch b2 = rest().get("trees/tree/test").as(Branch.class);
    rest().body(table)
           .queryParam("branch", b2.getName()).queryParam("hash", b2.getHash())
           .post("contents/xxx.test").then().statusCode(204);
    Contents returned = rest()
        .queryParam("ref", "test")
        .get("contents/xxx.test").then().statusCode(200).extract().as(Contents.class);
    Assertions.assertEquals(table, returned);

    Branch b3 = rest().get("trees/tree/test").as(Branch.class);
    rest().body(Tag.of("tagtest", b3.getHash())).post("trees/tree").then().statusCode(204);

    rest().get("trees/tree/tagtest").then().statusCode(200).body("hash", equalTo(b3.getHash()));

    rest().queryParam(
        "expectedHash",
        "0011223344556677889900112233445566778899001122334455667788990011".substring(0, b2.getHash().length()))
        .delete("trees/tag/tagtest").then().statusCode(409);

    rest().queryParam("expectedHash", b3.getHash())
        .delete("trees/tag/tagtest").then().statusCode(204);

    LogResponse log = rest().get("trees/tree/test/log").then().statusCode(200).extract().as(LogResponse.class);
    Assertions.assertEquals(3, log.getOperations().size());

    Branch b1 = rest().get("trees/tree/test").as(Branch.class);
    rest().queryParam("expectedHash", b1.getHash()).delete("trees/branch/test").then().statusCode(204);
    Branch bx = rest().get("trees/tree/mainx").as(Branch.class);
    rest().queryParam("expectedHash", bx.getHash()).delete("trees/branch/mainx").then().statusCode(204);
  }

  private static RequestSpecification rest() {
    return given().when().basePath("/api/v1/").contentType(ContentType.JSON);
  }

  private void commit(Branch b, String path, String metadataUrl) {
    rest()
      .body(IcebergTable.of(metadataUrl))
      .queryParam("branch", b.getName()).queryParam("hash", b.getHash())
      .post("contents/xxx.test")
      .then().statusCode(204);
  }

  private Branch getBranch(String name) {
    return rest().get("trees/tree/{name}", name).then().statusCode(200).extract().as(Branch.class);
  }

  private Branch makeBranch(String name) {
    Branch test = ImmutableBranch.builder()
        .name(name)
        .build();
    rest().body(test).post("trees/tree").then().statusCode(204);
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

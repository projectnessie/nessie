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
package org.projectnessie.jaxrs.tests;

import static org.projectnessie.jaxrs.ext.NessieJaxRsExtension.jaxRsExtensionForDatabaseAdapter;

import io.restassured.RestAssured;
import io.restassured.builder.RequestSpecBuilder;
import io.restassured.http.ContentType;
import java.net.URI;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.projectnessie.client.ext.NessieClientUri;
import org.projectnessie.jaxrs.ext.NessieJaxRsExtension;
import org.projectnessie.versioned.persist.adapter.DatabaseAdapter;
import org.projectnessie.versioned.persist.tests.extension.DatabaseAdapterExtension;
import org.projectnessie.versioned.persist.tests.extension.NessieDbAdapter;

@ExtendWith(DatabaseAdapterExtension.class)
abstract class AbstractTestDatabaseAdapterResteasyV1 extends AbstractResteasyV1Test {

  @NessieDbAdapter static DatabaseAdapter databaseAdapter;

  @RegisterExtension
  static NessieJaxRsExtension server = jaxRsExtensionForDatabaseAdapter(() -> databaseAdapter);

  @BeforeAll
  static void setup(@NessieClientUri URI uri) {
    RestAssured.baseURI = uri.toString();
    basePath = ""; // baseURI has the full base path
    RestAssured.port = uri.getPort();
    RestAssured.requestSpecification =
        new RequestSpecBuilder()
            .setContentType(ContentType.JSON)
            .setAccept(ContentType.JSON)
            .build();
  }
}

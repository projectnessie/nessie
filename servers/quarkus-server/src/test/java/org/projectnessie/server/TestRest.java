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

import java.net.URI;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.projectnessie.api.ContentsApi;
import org.projectnessie.api.TreeApi;
import org.projectnessie.client.NessieClient;
import org.projectnessie.client.http.HttpClient;
import org.projectnessie.client.rest.NessieHttpResponseFilter;
import org.projectnessie.services.rest.AbstractTestRest;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import io.quarkus.test.junit.QuarkusTest;

@QuarkusTest
class TestRest extends AbstractTestRest {

  private NessieClient client;
  private TreeApi tree;
  private ContentsApi contents;
  private HttpClient httpClient;

  @BeforeEach
  void init() {
    URI uri = URI.create("http://localhost:19121/api/v1");
    client = NessieClient.builder().withUri(uri).build();
    tree = client.getTreeApi();
    contents = client.getContentsApi();

    ObjectMapper mapper = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT)
        .disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);
    httpClient = HttpClient.builder().setBaseUri(uri).setObjectMapper(mapper).build();
    httpClient.register(new NessieHttpResponseFilter(mapper));
  }

  @AfterEach
  void closeClient() {
    client.close();
  }

  @Override
  protected NessieClient client() {
    return client;
  }

  @Override
  protected TreeApi tree() {
    return tree;
  }

  @Override
  protected ContentsApi contents() {
    return contents;
  }

  @Override
  protected HttpClient httpClient() {
    return httpClient;
  }
}

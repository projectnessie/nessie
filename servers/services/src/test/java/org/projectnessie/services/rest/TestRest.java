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
package org.projectnessie.services.rest;

import java.net.URI;
import java.security.Principal;

import javax.inject.Singleton;
import javax.ws.rs.core.Application;

import org.apache.commons.lang3.reflect.TypeUtils;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.JerseyTest;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.projectnessie.api.ContentsApi;
import org.projectnessie.api.TreeApi;
import org.projectnessie.client.NessieClient;
import org.projectnessie.client.http.HttpClient;
import org.projectnessie.client.rest.NessieHttpResponseFilter;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.Contents;
import org.projectnessie.services.config.ServerConfig;
import org.projectnessie.services.rest.inject.TestPrincipal;
import org.projectnessie.services.rest.inject.TestServerConfig;
import org.projectnessie.services.rest.inject.TestVersionStore;
import org.projectnessie.versioned.VersionStore;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

class TestRest extends AbstractTestRest {

  private static HttpTest jerseyTest;

  private static NessieClient client;
  private static TreeApi tree;
  private static ContentsApi contents;
  private static HttpClient httpClient;

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

  static class HttpTest extends JerseyTest {
    @Override
    protected Application configure() {
      ResourceConfig resourceConfig = new ResourceConfig();
      resourceConfig.register(new AbstractBinder() {
        @Override
        protected void configure() {
          bind(TestServerConfig.class)
              .to(ServerConfig.class)
              .in(Singleton.class);
          bind(TestVersionStore.class)
              .to(TypeUtils.parameterize(VersionStore.class, Contents.class, CommitMeta.class))
              .in(Singleton.class);
          bind(TestPrincipal.class)
              .to(Principal.class)
              .in(Singleton.class);
        }
      });
      resourceConfig.register(JacksonFeature.class);
      resourceConfig.packages("org.projectnessie.services.rest");
      resourceConfig.register(TreeResource.class);
      resourceConfig.register(ContentsResource.class);
      resourceConfig.register(ConfigResource.class);
      return resourceConfig;
    }

    @Override
    public URI getBaseUri() {
      return super.getBaseUri();
    }
  }

  @BeforeAll
  static void initialize() throws Exception {
    jerseyTest = new HttpTest();
    jerseyTest.setUp();

    URI uri = jerseyTest.getBaseUri();
    client = NessieClient.builder().withUri(uri).build();
    tree = client.getTreeApi();
    contents = client.getContentsApi();

    ObjectMapper mapper = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT)
        .disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);
    httpClient = HttpClient.builder().setBaseUri(uri).setObjectMapper(mapper).build();
    httpClient.register(new NessieHttpResponseFilter(mapper));
  }

  @AfterAll
  static void shutdown() throws Exception {
    jerseyTest.tearDown();
    client.close();
  }
}

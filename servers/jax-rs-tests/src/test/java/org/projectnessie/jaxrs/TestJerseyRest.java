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
package org.projectnessie.jaxrs;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import java.net.URI;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.projectnessie.client.HttpClientBuilder;
import org.projectnessie.client.http.HttpClient;
import org.projectnessie.client.rest.NessieHttpResponseFilter;

public class TestJerseyRest extends AbstractTestRest {
  @RegisterExtension static NessieJaxRsExtension server = new NessieJaxRsExtension();

  @Override
  @BeforeEach
  public void setUp() throws Exception {
    URI uri = URI.create(server.getURI().toString());

    ObjectMapper mapper =
        new ObjectMapper()
            .enable(SerializationFeature.INDENT_OUTPUT)
            .disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);
    HttpClient httpClient = HttpClient.builder().setBaseUri(uri).setObjectMapper(mapper).build();
    httpClient.register(new NessieHttpResponseFilter(mapper));
    super.init(HttpClientBuilder.builder().withUri(uri).build(), httpClient);
    super.setUp();
  }
}
